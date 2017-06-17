package org.example.recommand_search

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
    EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    /**
      * train은 content / cosmeitc 따로 받아 처리한다.
      * 각각에 대한 이벤트는 따로 정의 되어 있으므로 item으로 합쳐 전달되어도 구분가능하다.
      */
    // create a RDD of (entityID, content)
    val contentsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "content"
    )(sc).map { case (entityId, properties) =>
      val content = try {
        // Assume categories is optional property of content.
        Item(item_type = "content",
          categories = None,
          brand = None,
          cosmetics = properties.getOpt[Set[String]]("cosmetics"),
          upload_date = properties.getOpt[String]("upload_date")
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" content ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, content)
    }

    // create a RDD of (entityID, cosmetic)
    val cosmeticsRDD: RDD[(String, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "cosmetic"
    )(sc).map { case (entityId, properties) =>
      val cosmetic = try {
        // Assume categories is optional property of cosmetic.
        Item(item_type = "cosmetic",
          categories = properties.getOpt[Seq[String]]("categories"),
          brand = properties.getOpt[String]("brand"),
          cosmetics = None,
          upload_date = None
        )
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" cosmetic ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, cosmetic)
    }

    //content+cosmetic = item으로 합쳐 넘긴다
    val itemsRDD : RDD[(String, Item)] = contentsRDD.union(cosmeticsRDD).cache()


    /* content event들 정의 */
    val content_eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view","scrap","play","cancle_scrap")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("content")))(sc)
      .cache()

    val playEventsRDD: RDD[PlayEvent] = content_eventsRDD
      .filter { event => event.event == "play" }
      .map { event =>
        try {
          PlayEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to PlayEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val ContentViewEventsRDD = MakeViewEvent(content_eventsRDD)

    val ContentScrapEventsRDD = MakeScrapEvent(content_eventsRDD)

    /* cosmetic event들 정의 */
    val cosmetic_eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("own","view","scrap","cancle_scrap","rate")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("cosmetic")))(sc)
      .cache()


    val ownEventsRDD: RDD[OwnEvent] = cosmetic_eventsRDD
      .filter { event => event.event == "own" }
      .map { event =>
        try {
          OwnEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to OwnEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val rateEventsRDD: RDD[RateEvent] = cosmetic_eventsRDD
      .filter { event => event.event == "rate" }
      .map { event =>
        try {
          RateEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis,
            rating = event.properties.get[Double]("rating")
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to RateEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    val CosmeticViewEventsRDD = MakeViewEvent(cosmetic_eventsRDD)

    val CosmeticScrapEventsRDD = MakeScrapEvent(cosmetic_eventsRDD)

    /**
      * view & scrap 이벤트는 content & cosmetic 모두 있는 이벤트이므로 합산해준다.
      */
    val viewEventsRDD : RDD[ViewEvent] = ContentViewEventsRDD.union(CosmeticViewEventsRDD)

    val scrapEventsRDD : RDD[ScrapEvent] = ContentScrapEventsRDD.union(CosmeticScrapEventsRDD)

    new TrainingData(
      users = usersRDD,
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      ownEvents = ownEventsRDD,
      scrapEvents = scrapEventsRDD,
      rateEvents = rateEventsRDD,
      playEvents = playEventsRDD
    )
  }

  /*
  공통 이벤트 생성 함수 Scrap & View
   */
  def MakeViewEvent(eventsRDD : RDD[Event]): RDD[ViewEvent] = {
    eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId,
            item = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }
  }

  def MakeScrapEvent(eventsRDD : RDD[Event]): RDD[ScrapEvent] = {
    eventsRDD
      .filter { event => event.event == "scrap" | event.event == "cancel_scrap" } // MODIFIED
      .map { event =>
      try {
        ScrapEvent(
          user = event.entityId,
          item = event.targetEntityId.get,
          t = event.eventTime.getMillis,
          scrap = (event.event == "scrap")
        )
      } catch {
        case e: Exception =>
          logger.error(s"Cannot convert ${event} to ScrapEvent." +
            s" Exception: ${e}.")
          throw e
      }
    }
  }
}

case class User()

case class Item(item_type: String,
                categories: Option[Seq[String]], //순서열 main-sub순서
                brand: Option[String],
                cosmetics: Option[Set[String]],
                upload_date: Option[String]
)

case class ViewEvent(user: String, item: String, t: Long)

case class OwnEvent(user: String, item: String, t: Long)

case class RateEvent(user: String, item: String, t: Long, rating: Double)

case class PlayEvent(user: String, item: String, t: Long)

/**
  * @params scrap ( 화장품 찜 & 영상 스크랩 )
  *    true: scrap. false: cancel_scrap
  */
case class ScrapEvent(user: String, item: String, t: Long, scrap: Boolean)

class TrainingData(
                    val users: RDD[(String, User)],
                    val items: RDD[(String, Item)],
                    val viewEvents: RDD[ViewEvent],
                    val ownEvents: RDD[OwnEvent],
                    val scrapEvents: RDD[ScrapEvent],
                    val rateEvents: RDD[RateEvent],
                    val playEvents: RDD[PlayEvent]
                  ) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
      s"items: [${items.count()} (${items.take(2).toList}...)]" +
      s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
      s"ownEvents: [${ownEvents.count()}] (${ownEvents.take(2).toList}...)" +
      s"scrapEvents: [${scrapEvents.count()}] (${scrapEvents.take(2).toList}...)" +
      s"rateEvents: [${rateEvents.count()}] (${rateEvents.take(2).toList}...)" +
      s"playEvents: [${playEvents.count()}] (${playEvents.take(2).toList}...)"
  }
}
