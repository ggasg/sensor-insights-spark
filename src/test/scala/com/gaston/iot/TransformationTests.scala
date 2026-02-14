package com.gaston.iot

import com.gaston.iot.prep.JobProcessing

import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.types.{ArrayType, DoubleType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class TransformationTests extends AnyFunSuite with SharedSparkContext with Matchers {

  import spark.implicits._

  test("Should transform bronze to silver with proper types") {
    // Create mock bronze data
    val bronzeData = Seq(
      """{"timestamp":1000000000000000000,"reading":"{\"chip_id\":\"2050\",\"altitude\":100.5,\"pressure\":1013.25,\"temperature\":[20.0,68.0],\"location\":\"{\\\"lat\\\":37.7749,\\\"lon\\\":-122.4194}\"}"}"""
    ).toDF("payload")

    val jobProcessing = new JobProcessing(spark)
    val result = jobProcessing.transformToSilver(bronzeData)

    // Verify schema
    result.schema("altitude").dataType shouldBe DoubleType
    result.schema("pressure").dataType shouldBe DoubleType
    result.schema("temperature").dataType shouldBe ArrayType(DoubleType)
    result.schema("event_timestamp").dataType shouldBe TimestampType

    // Verify values
    val row = result.first()
    row.getAs[String]("chip_id") shouldBe "2050"
    row.getAs[Double]("altitude") shouldBe 100.5
    row.getAs[Double]("pressure") shouldBe 1013.25
    row.getAs[Seq[Double]]("temperature") shouldBe Seq(20.0, 68.0)

    // Check for nulls
    result.filter(col("altitude").isNull || col("pressure").isNull || col("temperature").isNull)
      .count() shouldBe 0
  }

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
