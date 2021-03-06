package org.example.recommand_search

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      viewEvents = trainingData.viewEvents,
      ownEvents = trainingData.ownEvents,
      scrapEvents = trainingData.scrapEvents,
      rateEvents = trainingData.rateEvents,
      playEvents = trainingData.playEvents
    )
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val items: RDD[(String, Item)],
  val viewEvents: RDD[ViewEvent],
  val ownEvents: RDD[OwnEvent],
  val scrapEvents: RDD[ScrapEvent],
  val rateEvents: RDD[RateEvent],
  val playEvents: RDD[PlayEvent]
) extends Serializable
