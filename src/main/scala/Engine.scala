package org.example.recommand_search

import org.apache.predictionio.controller.EngineFactory
import org.apache.predictionio.controller.Engine

case class Query(
  user: String,
  num: Int,
  item_type: String,
  search: Option[String],
  result_type: Option[String],
  cosmetic: Option[String],
  brand: Option[String],
  categories: Option[Set[String]],
  whiteList: Option[Set[String]],
  blackList: Option[Set[String]]
) extends Serializable

/**
  * 나중에 파라매터 추가해서 화장품 정보나 브랜드 정보 제공할 것
  */
case class PredictedResult(
  itemScores: Array[ItemScore]
) extends Serializable

case class ItemScore(
  item: String,
  score: Double
) extends Serializable

object RecommerceRecommendationEngine extends EngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("recomm" -> classOf[RecommAlgorithm],
      "coo" -> classOf[CooccurrenceAlgorithm]),
      classOf[Serving])
  }
}