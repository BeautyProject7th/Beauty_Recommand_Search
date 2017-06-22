package org.example.recommand_search

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD
import org.apache.spark.api.java.JavaSparkContext


import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

/**
  * --알고리즘 초기설정 파라미터--
  * appName: predictionIO 앱 이름
  * unseenOnly: unseen 이벤트만 보여줌
  * seenEvents: 유저가 본 이벤트의 user-to-item 리스트, unseenOnly가 true일 때 쓰임
  * similarEvents: 비슷한 이벤트의 user-item-item 리스트, 유저가 최근에 본 item과 비슷한 item을 찾을 때 쓰임
  * rank: MLlib ALS 알고리즘의 파라미터. Number of latent feature.
  * numIterations: MLlib ALS 알고리즘의 파라미터. Number of iterations.
  * lambda: MLlib ALS 알고리즘의 정규화 파라미터
  * seed: MLlib ALS 알고리즘의 random seed. (Optional)
  */
case class RecommAlgorithmParams(
                                  appName: String,
                                  unseenOnly: Boolean,
                                  seenEvents: List[String],
                                  similarEvents: List[String],
                                  rank: Int,
                                  numIterations: Int,
                                  lambda: Double,
                                  seed: Option[Long]
                                ) extends Params


case class ProductModel(
                         item: Item,
                         features: Option[Array[Double]], // features by ALS
                         count: Double // popular count for default score
                       )

class RecommModel(
                   val rank: Int,
                   val userFeatures: Map[Int, Array[Double]],
                   val productModels: Map[Int, ProductModel],
                   val userStringIntMap: BiMap[String, Int],
                   val itemStringIntMap: BiMap[String, Int]
                 ) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
      s" userFeatures: [${userFeatures.size}]" +
      s"(${userFeatures.take(2).toList}...)" +
      s" productModels: [${productModels.size}]" +
      s"(${productModels.take(2).toList}...)" +
      s" userStringIntMap: [${userStringIntMap.size}]" +
      s"(${userStringIntMap.take(2).toString}...)]" +
      s" itemStringIntMap: [${itemStringIntMap.size}]" +
      s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

class RecommAlgorithm(val ap: RecommAlgorithmParams)
  extends P2LAlgorithm[PreparedData, RecommModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): RecommModel = {
    /*
    require(!data.viewEvents.take(1).isEmpty,
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.ownEvents.take(1).isEmpty,
      s"ownEvents in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
    require(!data.scrapEvents.take(1).isEmpty,
      s"scrapEvents in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
      */
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
        " Please check if DataSource generates TrainingData" +
        " and Preprator generates PreparedData correctly.")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data,
      sc = sc
    )

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
        " Please check if your events contain valid user and item ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    val userFeatures = m.userFeatures.collectAsMap.toMap

    // convert ID to Int index
    val items = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }

    // join item with the trained productFeatures
    val productFeatures: Map[Int, (Item, Option[Array[Double]])] =
    items.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val popularCount = trainDefault(
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      data = data,
      sc = sc
    )

    val productModels: Map[Int, ProductModel] = productFeatures
      .map { case (index, (item, features)) =>
        val pm = ProductModel(
          item = item,
          features = features,
          // NOTE: use getOrElse because popularCount may not contain all items.
          count = popularCount.getOrElse(index, 0.0)
        )
        (index, pm)
      }

    new RecommModel(
      rank = m.rank,
      userFeatures = userFeatures,
      productModels = productModels,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap
    )
  }

  /** PreparedData로부터 MLlibRating을 생성
    * 각 이벤트에 맞는 item이 다르므로 mapping이 따로 됨
    * content : view, scrap
    * cosmetic : view, rate, own(3), scrap(2)
    *
    * cosmetic rate & own 둘다 존재하는 경우 공식
    * ( 별점(5.0만점) - 2.75(평균) ) * 3(소유) => -6.75 ~ +6.75
    * 소유한다고 모두 좋은 평은 아닐터, 0.5~5.0 별점 중 중간 값 기준 전은 좋지 않은 평가, 후는 좋은 평가로 생각
    * 즉 이 과정을 먼저 거친 후 나머지 값과는 합산해준다.
    * */
  def genMLlibRating(
                      userStringIntMap: BiMap[String, Int],
                      itemStringIntMap: BiMap[String, Int],
                      data: PreparedData,
                      sc: SparkContext): RDD[MLlibRating] = {

    val rateAndownRatings: RDD[((Int, Int), Double)] = Cal_RateOwnRatings(data,userStringIntMap,itemStringIntMap)

    val view_mllibRatings = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 1.0)
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair

    val play_mllibRatings = data.playEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 5.0)
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair

    // 'scrap' 이벤트에 대한 rating 생성
    val scrap_mllibRatings = data.scrapEvents
      .map { r =>
        // user와 item String ID를 Int로 변환
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        // key 는 (uindex, iindex) tuple, value 는 (scrap, t) tuple
        ((uindex, iindex), (r.scrap, r.t))
      }.filter { case ((u, i), v) =>
      // val  = d
      // 유효한 user와 item index에 해당하는 event를 keep
      (u != -1) && (i != -1)
    }.reduceByKey { case (v1, v2) =>
      // User가 scap를 했다가 cancel_scrap를 나중에 하게 되면, 또는 그 반대인 경우,
      // 가장 최근의 이벤트를 적용함.
      val (scrap1, t1) = v1
      val (scrap2, t2) = v2
      // keep the latest value
      if (t1 > t2) v1 else v2
    }.map { case ((u, i), (scrap, t)) =>
      // ALS.trainImplicit() 사용
      val r = if (scrap) 2.0 else 0.0  // 'scrap' 이벤트의 rating 은 5, 'cancel_scrap' 이벤트의 rating은 0
      ((u, i), r)
    }

    val sumOfmllibRatings = rateAndownRatings.union(view_mllibRatings).union(scrap_mllibRatings).union(play_mllibRatings)
      .reduceByKey(_ + _)
      .map { case ((u, i), v) =>
        // MLlibRating은 user와 item에 대한 integer index를 필요로 함
        MLlibRating(u, i, v)
      }.cache()

    sumOfmllibRatings
  }
  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for item.
    */
  def Cal_RateOwnRatings(data: PreparedData,
                         userStringIntMap: BiMap[String, Int],
                         itemStringIntMap: BiMap[String, Int]): RDD[((Int, Int), Double)] = {
    val own_mllibRatings = data.ownEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), 3.0)
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey(_ + _) // aggregate all view events of same user-item pair

    val rate_mllibRatings = data.rateEvents
      .map { r =>
        // Convert user and item String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), (r.rating, r.t))
      }
      .filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .reduceByKey { case (v1, v2) => // MODIFIED
        // if a user may rate same item with different value at different times,
        // use the latest value for this case.
        // Can remove this reduceByKey() if no need to support this case.
        val (rating1, t1) = v1
        val (rating2, t2) = v2
        // keep the latest value
        if (t1 > t2) v1 else v2
      }
      .map { case ((u, i), (rating, t)) => // MODIFIED
        // MLlibRating requires integer index for user and item
        ((u, i), rating - 2.75) // MODIFIED
      }

    val rateAndownRatings = own_mllibRatings.union(rate_mllibRatings).reduceByKey(_ * _)
    rateAndownRatings
  }

  /**
    * 이는 User에 대해 아무런 정보가 없을 때 대중적으로 선호하는 아이템을 추천하기 위함임.
    *
    * genMLibRating과 다른 점 : return 값에 user과 관련된 정보가 없음    *
    * genMLibRating과 다른 로직이 필요 : 추후 content, cosmetic 각각의 이벤트 비율을 달리해서 계산할 것 ( 꼭 안해도 될 것 같기도 하고 ㅎㅎ )
    * 예를 들어 : view 는 1, scrap는 5 ( genMLibRating은 1:1 )
    */
  def trainDefault(
                    userStringIntMap: BiMap[String, Int],
                    itemStringIntMap: BiMap[String, Int],
                    data: PreparedData,
                    sc: SparkContext): Map[Int, Double] = {

    val rateAndownCountsRDD: RDD[(Int, Double)] = Cal_RateOwnRatings(data,userStringIntMap,itemStringIntMap).map { case ((u,i),v) => (i, v)}

    val viewCountsRDD: RDD[(Int, Double)] = data.viewEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        (uindex, iindex, 1.0)
      }
      .filter { case (u, i, v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .map { case (u, i, v) => (i, 1.0) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    val play_mllibRatings = data.playEvents
      .map { r =>
        // Convert user and item String IDs to Int index
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        (uindex, iindex, 5.0)
      }
      .filter { case (u, i, v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .map { case (u, i, v) => (i, 1.0) } // key is item
      .reduceByKey{ case (a, b) => a + b } // count number of items occurrence

    val scrapCountsRDD: RDD[(Int, Double)] = data.scrapEvents
      .map { r =>
        // user와 item String ID를 Int로 변환
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          print(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (iindex == -1)
          print(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        // key 는 (uindex, iindex) tuple, value 는 (scrap, t) tuple
        ((uindex, iindex), (r.scrap, r.t))
      }
      .filter { case ((u, i), v) =>
        // 유효한 user와 item index에 해당하는 event를 keep
        (u != -1) && (i != -1)
      }
      .map { case ((u, i), (scrap, t)) =>
        if (scrap) (i, 2.0) else (i, -2.0) // scrap: 3, cancel_scrap: -3, cancel_scrap의 경우 -5를 count하여 scrap 상쇄함.
      } // key is item
      .reduceByKey(_ + _) // 같은 user_item pair에 대해 모든 'scrap', 'cancel_scrap' 이벤트를 합침

    val sumOfCountsRDD = rateAndownCountsRDD.union(viewCountsRDD).union(scrapCountsRDD).union(play_mllibRatings)
      .reduceByKey(_ + _).collectAsMap.toMap

    sumOfCountsRDD
  }

  def predict(model: RecommModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val productModels = model.productModels

    // convert whiteList's string ID to integer index
    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.flatMap(model.itemStringIntMap.get(_))
    )

    val finalBlackList: Set[Int] = genBlackList(query = query)
      // convert seen Items list from String ID to interger Index
      .flatMap(x => model.itemStringIntMap.get(x))

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex =>
        userFeatures.get(userIndex)
      }

    val queryList: Set[String] = query.search match {
      case Some(s) => s.split(" ").toSet
      case None => Set()
    }
    val topScores: Array[(Int, Double)] = query.result_type match {
      case Some(r) =>
        logger.info(s"predict popluar.")
        predictDefault(
          productModels = productModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList,
          itemIntStringMap = model.itemIntStringMap,
          queryList = queryList
        )
      case None =>
        if (userFeature.isDefined) {
          // the user has feature vector
          logger.info(s"predict knownuser found for user ${query.user}.")
          predictKnownUser(
            userFeature = userFeature.get,
            productModels = productModels,
            query = query,
            whiteList = whiteList,
            blackList = finalBlackList,
            itemIntStringMap = model.itemIntStringMap,
            queryList = queryList
          )
        } else {
          // the user doesn't have feature vector.
          // For example, new user is created after model is trained.
          logger.info(s"No userFeature found for user ${query.user}.")

          // check if the user has recent events on some items
          val recentItems: Set[String] = getRecentItems(query)
          val recentList: Set[Int] = recentItems.flatMap(x =>
            model.itemStringIntMap.get(x))

          val recentFeatures: Vector[Array[Double]] = recentList.toVector
            // productModels may not contain the requested item
            .map { i =>
            productModels.get(i).flatMap { pm => pm.features }
          }.flatten

          if (recentFeatures.isEmpty) {
            logger.info(s"No features vector for recent items ${recentItems}.")
            predictDefault(
              productModels = productModels,
              query = query,
              whiteList = whiteList,
              blackList = finalBlackList,
              itemIntStringMap = model.itemIntStringMap,
              queryList = queryList
            )
          } else {
            predictSimilar(
              recentFeatures = recentFeatures,
              productModels = productModels,
              query = query,
              whiteList = whiteList,
              blackList = finalBlackList,
              itemIntStringMap = model.itemIntStringMap,
              queryList = queryList
            )
          }
        }
    }


    val itemScores = topScores.map { case (i, s) =>
      //logger.info(s"result : ${model.itemIntStringMap(i)}")

      new ItemScore(
        // convert item int index back to string ID
        item = model.itemIntStringMap(i),
        score = s
      )
    }

    new PredictedResult(itemScores)
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen items
    val seenItems: Set[String] = if (ap.unseenOnly) {

      // get all user item events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("item")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e: Exception => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    // get the latest constraint unavailableItems $set event
    val unavailableItems: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableItems",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("items")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableItems event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableItems event: ${e}")
        throw e
    }

    // combine query's blackList,seenItems and unavailableItems
    // into final blackList.
    query.blackList.getOrElse(Set[String]()) ++ seenItems ++ unavailableItems
  }

  /** Get recent events of the user on items for recommending similar items */
  def getRecentItems(query: Query): Set[String] = {
    /*TODO:
     할일 6 이런식으로 하거나 engine.json에서 파라매타 추가해서 가져오기
    var item_type = "content"
    var similarEvent  = Seq("view","play","scrap")
    if(query.item_type == "cosmetic"){
      item_type = "cosmetic"
      similarEvent = Seq("view","own","rate","scrap")
    }
    print(s"$similarEvent")
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(similarEvent),
        targetEntityType = Some(Some(item_type)),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }
    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Exception => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet
    recentItems
     */
    // get latest 10 user view item events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("item")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentItems: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e: Exception => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentItems
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
                        userFeature: Array[Double],
                        productModels: Map[Int, ProductModel],
                        query: Query,
                        whiteList: Option[Set[Int]],
                        blackList: Set[Int],
                        itemIntStringMap: BiMap[Int, String],
                        queryList: Set[String]
                      ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
      pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          item_type = query.item_type,
          cosmetic = query.cosmetic,
          brand = query.brand,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          itemIntStringMap = itemIntStringMap,
          queryList = queryList
        )
    }
      .map { case (i, pm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, pm.features.get)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // only keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Default prediction when know nothing about the user */
  def predictDefault(
                      productModels: Map[Int, ProductModel],
                      query: Query,
                      whiteList: Option[Set[Int]],
                      blackList: Set[Int],
                      itemIntStringMap: BiMap[Int, String],
                      queryList: Set[String]
                    ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert back to sequential collection
      .filter { case (i, pm) =>
      isCandidateItem(
        i = i,
        item = pm.item,
        item_type = query.item_type,
        cosmetic = query.cosmetic,
        brand = query.brand,
        categories = query.categories,
        whiteList = whiteList,
        blackList = blackList,
        itemIntStringMap = itemIntStringMap,
        queryList = queryList
      )
    }
      .map { case (i, pm) =>
        // may customize here to further adjust score
        (i, pm.count.toDouble)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Return top similar items based on items user recently has action on */
  def predictSimilar(
                      recentFeatures: Vector[Array[Double]],
                      productModels: Map[Int, ProductModel],
                      query: Query,
                      whiteList: Option[Set[Int]],
                      blackList: Set[Int],
                      itemIntStringMap: BiMap[Int, String],
                      queryList: Set[String]
                    ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = productModels.par // convert to parallel collection
      .filter { case (i, pm) =>
      pm.features.isDefined &&
        isCandidateItem(
          i = i,
          item = pm.item,
          item_type = query.item_type,
          cosmetic = query.cosmetic,
          brand = query.brand,
          categories = query.categories,
          whiteList = whiteList,
          blackList = blackList,
          itemIntStringMap = itemIntStringMap,
          queryList = queryList
        )
    }
      .map { case (i, pm) =>
        val s = recentFeatures.map{ rf =>
          // pm.features must be defined because of filter logic above
          cosine(rf, pm.features.get)
        }.reduce(_ + _)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // keep items with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  private
  def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateItem(
                       i: Int,
                       item: Item,
                       item_type: String,
                       cosmetic: Option[String],
                       brand: Option[String],
                       categories: Option[Set[String]],
                       whiteList: Option[Set[Int]],
                       blackList: Set[Int],
                       itemIntStringMap: BiMap[Int, String],
                       queryList: Set[String]
                     ): Boolean = {
    var query_flag = true
    if(item.item_type == "cosmetic" && item.item_type == item_type) {
      if(queryList.contains("")) query_flag = false
      else {
        queryList.map { s =>
          if (!itemIntStringMap(i).replaceAll(" ", "").contains(s)) {
            query_flag = false
          }else logger.info(s"cosmetic ${itemIntStringMap(i).replaceAll(" ", "")} contain ${s}")
        }
      }
    }else if(item.item_type == "content" && item.item_type == item_type){
      //아래 1,2는 겹칠 일이 없음
      //1.화장품 상세 - 영상 추천인 경우 ( 쿼리의 화장품이 포함된 결과만 리턴 )
      if(cosmetic != None){
        if(item.cosmetics.get.contains(cosmetic.get)){
          query_flag = true
          logger.info(s"${itemIntStringMap(i)} 's cosmetics : ${item.cosmetics.get}")
        }else query_flag = false
      }else {
        //2.검색한 경우 검색어 필터링
        if(item.cosmetics.get.size > 0) { //화장품 배열에 뭐 든 경우만.
        val contain_query = item.cosmetics.get
          .map { c =>
            var flag = true
            queryList.map { s =>
              try {
                if (!c.replaceAll(" ", "").contains(s)) {
                  flag = false
                } else logger.info(s"content's cosmetic ${c.replaceAll(" ", "")} contain ${s}")
              } catch {
                case e: Exception =>
                  println("Error: " + e.getMessage)
                  flag = false
              }
            }
            flag
          }.toSet
          if (contain_query.contains(true)) {
            query_flag = true
          } else {
            query_flag = false
          }
        }else query_flag = false
      }
    }

    //if(query_flag) logger.info(s"${itemIntStringMap(i)} 통과했어염")

    var brand_flag : Boolean = {
      if(brand == None) true
      else (brand == item.brand)
    }

    query_flag && brand_flag &&
      whiteList.map(_.contains(i)).getOrElse(true) &&
      !blackList.contains(i) &&
      // filter categories
      categories.map { cat =>
        item.categories.map { itemCat =>
          // keep this item if has ovelap categories with the query
          !(itemCat.toSet.intersect(cat).isEmpty)
        }.getOrElse(false) // discard this item if it has no categories
      }.getOrElse(true) && (item_type == item.item_type)
  }
}