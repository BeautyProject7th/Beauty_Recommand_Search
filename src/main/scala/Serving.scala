package org.example.recommand_search

import org.apache.predictionio.controller.LServing

class Serving
  extends LServing[Query, PredictedResult] {

  override
  def serve(query: Query,
            predictedResults: Seq[PredictedResult]): PredictedResult = {

    // MODFIED
    val standard: Seq[Array[ItemScore]] = predictedResults.map(_.itemScores)

    // sum the standardized score if same item
    val combined = standard.flatten // Array of ItemScore
      .groupBy(_.item) // groupBy item id
      .mapValues(itemScores => itemScores.map(_.score).reduce(_ + _))
      .toArray // array of (item id, score)
      .sortBy(_._2)(Ordering.Double.reverse)
      .take(query.num)
      .map { case (k,v) => ItemScore(k, v) }

    new PredictedResult(combined)
  }
}
