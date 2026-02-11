package com.gaston.iot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TransformationTests extends AnyFunSuite with SharedSparkContext with Matchers {

  import spark.implicits._

  test("Should compute 5-minute window averages correctly") {
    val baseTime = 1000000000000000000L
    val oneMinuteNs = 60L * 1000000000L
    val fiveMinutesNs = 300L * 1000000000L

    val testData = Seq(
      (baseTime, Array(20.0, 68.0), 100.0, 1013.0),                      // Window 1
      (baseTime + oneMinuteNs, Array(22.0, 71.6), 105.0, 1012.0),        // Window 1 (1 min later)
      (baseTime + fiveMinutesNs, Array(25.0, 77.0), 110.0, 1011.0),      // Window 2 (5 min later)
      (baseTime + 2 * fiveMinutesNs, Array(28.0, 82.4), 115.0, 1010.0)   // Window 3 (10 min later)
    ).toDF("event_timestamp", "temperature", "altitude", "pressure")

    val jobProcessing = new JobProcessing(spark)
    val result = jobProcessing.computeWindowAverages(testData)

    result.count() shouldBe 3

    val windows = result.orderBy("window.start").collect()
    windows(0).getAs[Double]("avg_c") shouldBe 21.0 +- 0.1  // avg(20.0, 22.0)
    windows(1).getAs[Double]("avg_c") shouldBe 25.0 +- 0.1
    windows(2).getAs[Double]("avg_c") shouldBe 28.0 +- 0.1
  }

}
