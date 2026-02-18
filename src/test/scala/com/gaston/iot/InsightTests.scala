package com.gaston.iot

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.gaston.iot.insight.AnomalyDetection
import org.apache.spark.sql.functions.{col, mean, stddev}
import org.slf4j.LoggerFactory

import java.sql.Timestamp

class InsightTests extends AnyFunSuite with SharedSparkContext with Matchers {

  import spark.implicits._
  private val logger = LoggerFactory.getLogger(getClass)

  test("Should detect sudden temperature jumps as anomalies") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(20.5, 68.9), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:02:00"), "sensor1", Array(25.0, 77.0), 100.0, 1013.0),  // +4.5°C jump - anomaly!
      (Timestamp.valueOf("2026-02-17 14:03:00"), "sensor1", Array(20.8, 69.4), 100.0, 1013.0)   // -4.2°C jump - also anomaly!
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 2  // Both jumps detected

    // Verify the two anomalies
    anomalies(0).getAs[Seq[Double]]("temperature")(0) shouldBe 25.0
    Math.abs(anomalies(0).getAs[Double]("temp_change_rate")) should be > 3.0

    anomalies(1).getAs[Seq[Double]]("temperature")(0) shouldBe 20.8
    Math.abs(anomalies(1).getAs[Double]("temp_change_rate")) should be > 3.0
  }

  test("Should detect negative temperature as anomaly") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(-10.0, 14.0), 100.0, 1013.0)  // Negative temp - anomaly!
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[Seq[Double]]("temperature")(0) shouldBe -10.0
  }

  test("Should detect impossible temperature values as anomalies") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(150.0, 302.0), 100.0, 1013.0)  // 150°C - impossible!
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[Seq[Double]]("temperature")(0) shouldBe 150.0
  }

  test("Should detect zero values (sensor failure) as anomalies") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(0.0, 0.0), 100.0, 0.0)  // Sensor failure!
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[Seq[Double]]("temperature")(0) shouldBe 0.0
    anomalies(0).getAs[Double]("pressure") shouldBe 0.0
  }

  test("Should NOT flag normal temperature variations as anomalies") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(20.5, 68.9), 100.0, 1012.5),
      (Timestamp.valueOf("2026-02-17 14:02:00"), "sensor1", Array(21.0, 69.8), 100.0, 1012.0),
      (Timestamp.valueOf("2026-02-17 14:03:00"), "sensor1", Array(20.8, 69.4), 100.0, 1011.8)
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    result.filter(col("is_anomaly") === true).count() shouldBe 0
  }

  test("Should detect anomalies per sensor independently") {
    val testData = Seq(
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor1", Array(20.0, 68.0), 100.0, 1013.0),
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor1", Array(25.0, 77.0), 100.0, 1013.0),  // sensor1: +5°C jump (anomaly)
      (Timestamp.valueOf("2026-02-17 14:00:00"), "sensor2", Array(20.0, 68.0), 100.0, 1013.0),  // Changed from 15.0 to 20.0
      (Timestamp.valueOf("2026-02-17 14:01:00"), "sensor2", Array(20.5, 68.9), 100.0, 1013.0)   // Changed from 15.5 to 20.5 (normal)
    ).toDF("event_timestamp", "chip_id", "temperature", "altitude", "pressure")

    val result = AnomalyDetection.detectAnomaliesRateChange(testData)

    val anomalies = result.filter(col("is_anomaly") === true).collect()
    anomalies.length shouldBe 1
    anomalies(0).getAs[String]("chip_id") shouldBe "sensor1"
  }
}
