package com.gaston.iot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.gaston.iot.insight.AnomalyDetection
import org.apache.spark.sql.functions.{col, mean, stddev}
import org.slf4j.LoggerFactory

/*
  Z-score anomaly detection requires sufficient normal data to establish a reliable baseline.
  With small datasets, outliers significantly affect the mean/stddev they're being compared against.
  Rule of thumb: Need at least 15-20 normal observations for z-score > 3.0 detection to work reliably.
 */
class InsightTests extends AnyFunSuite with SharedSparkContext with Matchers {

  import spark.implicits._
  private val logger = LoggerFactory.getLogger(getClass)

  test("Should detect temperature anomalies using z-score") {
    val baseTime = 1000000000000000000L
    val oneMinuteNs = 60L * 1000000000L

    // Create 20 normal readings
    val normalReadings = (0 until 20).map { i =>
      ("sensor1", baseTime + i * oneMinuteNs, Array(20.0 + (i % 3) * 0.5, 68.0), 100.0, 1013.0)
    }

    // Add 1 extreme anomaly
    val anomalyReading = Seq(
      ("sensor1", baseTime + 20 * oneMinuteNs, Array(150.0, 302.0), 106.0, 1011.0)
    )

    val testData = (normalReadings ++ anomalyReading)
      .toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    logger.debug(s"Mean: ${result.select(mean("temp_c")).first().getDouble(0)}")
    logger.debug(s"Stddev: ${result.select(stddev("temp_c")).first().getDouble(0)}")
//    result.filter(col("temp_c") === 150.0).select("temp_zscore").show()

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[Seq[Double]]("temperature")(0) shouldBe 150.0
  }

  test("Should detect pressure anomalies") {
    val baseTime = 1000000000000000000L
    val oneMinuteNs = 60L * 1000000000L

    // Create 20 normal readings
    val normalReadings = (0 until 20).map { i =>
      ("sensor1", baseTime + i * oneMinuteNs, Array(20.0, 68.0), 100.0, 1012.0 + (i % 3) * 0.5)
    }

    // Add 1 pressure anomaly
    val anomalyReading = Seq(
      ("sensor1", baseTime + 20 * oneMinuteNs, Array(20.5, 68.9), 100.0, 500.0)  // Extreme low pressure
    )

    val testData = (normalReadings ++ anomalyReading)
      .toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[Double]("pressure") shouldBe 500.0
    anomalies(0).getAs[Double]("pressure_zscore") should be > 3.0
  }

  test("Should handle single row batch without errors") {
    val testData = Seq(
      ("sensor1", 1000000000000000000L, Array(20.0, 68.0), 100.0, 1013.0)
    ).toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    // Should not crash, stddev will be null but handled
    result.count() shouldBe 1
    result.filter(col("is_anomaly") === true).count() shouldBe 0  // No anomaly with single point
  }

  test("Should handle identical values without division by zero") {
    val baseTime = 1000000000000000000L
    val oneMinuteNs = 60L * 1000000000L

    val testData = Seq(
      ("sensor1", baseTime, Array(20.0, 68.0), 100.0, 1013.0),
      ("sensor1", baseTime + oneMinuteNs, Array(20.0, 68.0), 100.0, 1013.0),
      ("sensor1", baseTime + 2 * oneMinuteNs, Array(20.0, 68.0), 100.0, 1013.0)
    ).toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    // Should not crash (stddev = 0, handled by replacing with 1.0)
    result.count() shouldBe 3
    result.filter(col("is_anomaly") === true).count() shouldBe 0
  }

  test("Should calculate temperature change rate per sensor") {
    val baseTime = 1000000000000000000L
    val oneMinuteNs = 60L * 1000000000L

    val testData = Seq(
      ("sensor1", baseTime, Array(20.0, 68.0), 100.0, 1013.0),
      ("sensor1", baseTime + oneMinuteNs, Array(25.0, 77.0), 100.0, 1013.0),  // +5 degrees
      ("sensor2", baseTime, Array(15.0, 59.0), 100.0, 1013.0),
      ("sensor2", baseTime + oneMinuteNs, Array(16.0, 60.8), 100.0, 1013.0)   // +1 degree
    ).toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    // Verify change rates are calculated per sensor
    val sensor1Rates = result.filter(col("chip_id") === "sensor1" && col("temp_change_rate") =!= 0.0).collect()
    val sensor2Rates = result.filter(col("chip_id") === "sensor2" && col("temp_change_rate") =!= 0.0).collect()

    sensor1Rates.length shouldBe 1
    sensor1Rates(0).getAs[Double]("temp_change_rate") shouldBe 5.0 +- 0.01

    sensor2Rates.length shouldBe 1
    sensor2Rates(0).getAs[Double]("temp_change_rate") shouldBe 1.0 +- 0.01
  }

  test("Should include required output columns") {
    val testData = Seq(
      ("sensor1", 1000000000000000000L, Array(20.0, 68.0), 100.0, 1013.0)
    ).toDF("chip_id", "event_timestamp", "temperature", "altitude", "pressure")
      .withColumn("event_timestamp", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))

    val result = AnomalyDetection.detectAnomalies(testData)

    // Verify expected columns exist
    result.columns should contain allOf("temp_zscore", "pressure_zscore", "is_anomaly", "temp_change_rate", "pressure_change_rate")
  }

}
