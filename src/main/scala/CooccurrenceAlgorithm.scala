package org.example.recommand_search

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

/**
  * 1. 문제 소개 - 공기(co-occurrence) 정보
공기(共起, air가 아님!)란 단어와 단어가 하나의 문서나 문장에서 함께 쓰이는 현상을 얘기한다. 야구와 박찬호는 공기할 확률이 높다. 세탁기와 짜장면은 공기할 확률이 작다.
굳이 자세히 설명하지 않아도, 말뭉치에서 단어들 간의 공기 횟수를 뽑아 놓으면 두루 두루 쓰이는 곳이 많다.
파이썬 스크립트 co_occur_count_local.py는 분산이 아닌 단일 머신 환경에서 한개의 파일로 부터 단어쌍의 공기 횟수를 수집하는 프로그램이다.
  */

case class CooccurrenceAlgorithmParams(
                                        n: Int // top co-occurrence
                                      ) extends Params

class CooccurrenceModel(
                         val topCooccurrences: Map[Int, Array[(Int, Double)]],
                         val itemStringIntMap: BiMap[String, Int],
                         val userStringIntMap: BiMap[String, Int],
                         val items: Map[Int, Item]
                       ) extends Serializable {
  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString(): String = {
    val s = topCooccurrences.mapValues { v => v.mkString(",") }
    s.toString
  }
}

class CooccurrenceAlgorithm(val ap: CooccurrenceAlgorithmParams)
  extends P2LAlgorithm[PreparedData, CooccurrenceModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]
  def train(sc: SparkContext, data: PreparedData): CooccurrenceModel = {

    val itemStringIntMap = BiMap.stringInt(data.items.keys)
    val userStringIntMap = BiMap.stringInt(data.users.keys)

    val contentCosmetics = data.items.map {
      case (id, item) =>
        item.cosmetics match {
          case Some(cosmetics) => (itemStringIntMap(id), cosmetics)
          case None => (itemStringIntMap(id), Set())
        }
    }.filter(!_._2.isEmpty)
      .map {
        case (i, cosmetics) =>
          val filter_cosmetics = cosmetics.map {
            i => itemStringIntMap.getOrElse(i, -1)
          }.filter(_ != -1)

          (i, filter_cosmetics)
      }.collectAsMap.toMap

    val topCooccurrences =  trainCooccurrence(
      events = data.ownEvents,
      //n = ap.n,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap,
      contentCosmetics = contentCosmetics
    )

    // collect Item as Map and convert ID to Int index
    val items: Map[Int, Item] = data.items.map { case (id, item) =>
      (itemStringIntMap(id), item)
    }.collectAsMap.toMap

    new CooccurrenceModel(
      topCooccurrences = topCooccurrences,
      itemStringIntMap = itemStringIntMap,
      userStringIntMap = userStringIntMap,
      items = items
    )

  }

  def trainCooccurrence(
                         events: RDD[OwnEvent],
                         //n: Int,
                         userStringIntMap: BiMap[String, Int],
                         itemStringIntMap: BiMap[String, Int],
                         contentCosmetics: Map[Int, Set[Int]]): Map[Int, Array[(Int, Double)]] = {

    /**
      * 유저가 가진 화장품 리스트들을 ownEvents로부터 가져온다
      * 화장품, 유 명 int로 바뀜
      */
    val userCosmetics = events
      // map item from string to integer index
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

      (uindex, iindex)
    }
      .filter { case (u, i) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }
      .groupByKey
      .cache()
    /**
      * contentCosmetic 와 userCosmetic 중 겹치는 것이 있을 때 +1을 해준다
      */

    //var cooccurrences : Map[Int, Array[(Int, Double)]] = Nil

    val topCooccurrences = userCosmetics
      .map { case (u, user_cs) =>
        val contentAndScore = contentCosmetics.map{
          case (i, content_cs) =>
            val score : Double = content_cs.toSet.intersect(user_cs.toSet).size * 5
            (i, score)
        }.toArray

        /*count가 큰 순서대로 바꿔준다.
        take(n)은 배열 앞에서 부터 n개까지만 가져온다는 뜻
        engine.json의 파라매터에서 설정가능하다. )
         */
        (u, contentAndScore)
      }
      .collectAsMap.toMap

    topCooccurrences
  }

  def predict(model: CooccurrenceModel, query: Query): PredictedResult = {

    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val blackList: Option[Set[Int]] = query.blackList.map ( set =>
      set.map(model.itemStringIntMap.get(_)).flatten
    )

    val itemZeroScore: Map[Int, Double] = model.items.filter{
      case (i,item) => (query.item_type == item.item_type)
    }.map{
      case (i,item) => (i, 0.0)
    }

    val content_counts: Map[Int, Double] = model.topCooccurrences
      .getOrElse(model.userStringIntMap(query.user), Array()).toMap
    //.map { case (index, indexCounts) => (index, indexCounts.map(_._2).sum) }

    val query_list: Option[Set[String]] = query.search match {
      case Some(s) => Some(s.split(" ").toSet)
      case None => None
    }

    val counts: Array[(Int, Double)] = {
      if (query.item_type == "content"){
        query.search match {
          case Some(s) =>
            val sum_array = itemZeroScore.map{
              case (i,score) => (i, score+content_counts.getOrElse(i,0.0))
            }
            sum_array.toArray
          case None =>
            query.result_type match {
              case Some(r) =>
                //popluar일때
                itemZeroScore.toArray
              case None => content_counts.toArray
            }
        }
      }
      else if(query.item_type == "cosmetic"){
        itemZeroScore.toArray
        /*
        query.search match {
          case Some(s) =>itemZeroScore.toArray
          case None => Array()
        }
        */
      }else Array()
    }

    val itemScores = counts
      .filter { case (i, v) =>
        isCandidateItem(
          i = i,
          items = model.items,
          categories = query.categories,
          cosmetic = query.cosmetic,
          whiteList = whiteList,
          blackList = blackList,
          queryList = query_list,
          brand = query.brand,
          itemIntStringMap = model.itemIntStringMap
        )
      }
      .sortBy(_._2)(Ordering.Double.reverse)
      .take(query.num)
      .map { case (index, count) =>
        ItemScore(
          item = model.itemIntStringMap(index),
          score = count
        )
      }

    new PredictedResult(itemScores)
  }

  private
  def isCandidateItem(
                       i: Int,
                       items: Map[Int, Item],
                       categories: Option[Set[String]],
                       cosmetic: Option[String],
                       whiteList: Option[Set[Int]],
                       blackList: Option[Set[Int]],
                       queryList: Option[Set[String]],
                       brand: Option[String],
                       itemIntStringMap: BiMap[Int, String]
                     ): Boolean = {

    var query_flag = true
    if(items(i).item_type == "cosmetic") {
      queryList match {
        case Some(set) =>
          //popluar일때
          if(set.contains("")) query_flag = false
          else {
            set.map { s =>
              if (!itemIntStringMap(i).replaceAll(" ", "").contains(s)) {
                query_flag = false
                logger.info(s"cosmetic ${itemIntStringMap(i).replaceAll(" ", "")} not contain ${s}")
              }
            }
          }
        case None => query_flag = true
      }
    }else if(items(i).item_type == "content"){
      queryList match {
        case Some(set) =>

          //아래 1,2는 겹칠 일이 없음
          //1.화장품 상세 - 영상 추천인 경우 ( 쿼리의 화장품이 포함된 결과만 리턴 )
          if(cosmetic != None){
            query_flag = items(i).cosmetics.get.contains(cosmetic.get)
            print(s"${itemIntStringMap(i)} result : ${query_flag}")
          }else {
            //2.검색한 경우 검색어 필터링
            val contain_query = items(i).cosmetics.get
              .map { c =>
                var flag = true
                queryList.map { s =>
                  if (!c.replaceAll(" ", "").contains(s)) {
                    flag = false
                    logger.info(s"content's cosmetic ${c.replaceAll(" ", "")} not contain ${s}")
                  }
                }
                flag
              }.toSet
            if (contain_query.contains(true)) {
              query_flag = true
            } else {
              query_flag = false
            }
          }
        case None => query_flag = true
      }
    }


    var brand_flag : Boolean = {
      if(brand == None) true
      else (brand == items(i).brand)
    }

    query_flag && brand_flag &&
    whiteList.map(_.contains(i)).getOrElse(true) &&
      blackList.map(!_.contains(i)).getOrElse(true) &&
      // discard items in query as well
      // filter categories
      categories.map { cat =>
        items(i).categories.map { itemCat =>
          // keep this item if has ovelap categories with the query
          !(itemCat.toSet.intersect(cat).isEmpty)
        }.getOrElse(false) // discard this item if it has no categories
      }.getOrElse(true)
  }

}