package org.example.recommand_search

import org.scalatest.FlatSpec
import org.scalatest.Matchers
/*
class PreparatorTest
  extends FlatSpec with EngineTestSparkContext with Matchers {

  val preparator = new Preparator()
  val users = Map(
    "u0" -> User(),
    "u1" -> User()
  )

  val items = Map(
    "i0" -> Item(item_type = "content", categories = Some(Seq("c0", "c1")), brand = None, cosmetics = None, upload_date = None),
    "i1" -> Item(item_type = "content", categories = None, brand = None, cosmetics = None, upload_date = None)
  )

  val view = Seq(
    ViewEvent("u0", "i0", 1000010),
    ViewEvent("u0", "i1", 1000020),
    ViewEvent("u1", "i1", 1000030)
  )

  val own = Seq(
    OwnEvent("u0", "i0", 1000020),
    OwnEvent("u0", "i1", 1000030),
    OwnEvent("u1", "i1", 1000040)
  )

  val scrap = Seq(
    ScrapEvent("u0", "i0", 1000030, true),
    ScrapEvent("u0", "i1", 1000040, false),
    ScrapEvent("u0", "i1", 1000050, true)
  )

  val rate = Seq(
    RateEvent("u0", "i0", 1000020, 0.5),
    RateEvent("u0", "i1", 1000030, 5.0),
    RateEvent("u1", "i1", 1000040, 3.5)
  )

  val play = Seq(
    PlayEvent("u0", "i0", 1000020),
    PlayEvent("u0", "i1", 1000030),
    PlayEvent("u1", "i1", 1000040)
  )

  // simple test for demonstration purpose
  "Preparator" should "prepare PreparedData" in {

    val trainingData = new TrainingData(
      users = sc.parallelize(users.toSeq),
      items = sc.parallelize(items.toSeq),
      viewEvents = sc.parallelize(view.toSeq),
      ownEvents = sc.parallelize(own.toSeq),
      scrapEvents = sc.parallelize(scrap.toSeq),
      rateEvents = sc.parallelize(rate.toSeq),
      playEvents = sc.parallelize(play.toSeq)
    )

    val preparedData = preparator.prepare(sc, trainingData)

    preparedData.users.collect should contain theSameElementsAs users
    preparedData.items.collect should contain theSameElementsAs items
    preparedData.viewEvents.collect should contain theSameElementsAs view
    preparedData.ownEvents.collect should contain theSameElementsAs own
  }
}
*/