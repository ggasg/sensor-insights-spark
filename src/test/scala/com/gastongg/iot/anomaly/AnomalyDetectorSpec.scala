package com.gastongg.iot.anomaly

import com.gastongg.iot.SparkTestSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.LocalDateTime

class AnomalyDetectorSpec extends AnyFunSuite with Matchers {

  private val spark = SparkTestSession.spark

  private val windowStart = LocalDateTime.of(2024, 1, 1, 0, 0, 0)

  private def readings(count: Int, outlierTemp: Double, chipId: Int = 1): Seq[(Timestamp, Int, Double, Double)] =
    (0 until count).map { i =>
      val temp = if (i == count - 1) outlierTemp else 20.0
      (Timestamp.valueOf(windowStart.plusSeconds(i)), chipId, temp, 1000.0)
    }

  test("flags an obvious outlier once enough samples have accumulated in the window") {
    import spark.implicits._
    val df = readings(count = 30, outlierTemp = 100.0)
      .toDF("event_time", "chip_id", "temperature_celsius", "pressure")

    val result = AnomalyDetector.detect(
      df,
      windowDuration = "10 minutes",
      slideDuration = "10 minutes",
      zThreshold = 3.0,
      minSamples = 10
    ).collect()

    result should have length 1
    val row = result.head
    row.getAs[Long]("sample_count") shouldBe 30L
    row.getAs[Boolean]("is_anomaly") shouldBe true
    row.getAs[Double]("temp_z_score") should be > 3.0
  }

  test("does not flag a window that hasn't reached min-samples yet, even with a clear outlier") {
    import spark.implicits._
    val df = readings(count = 5, outlierTemp = 100.0)
      .toDF("event_time", "chip_id", "temperature_celsius", "pressure")

    val result = AnomalyDetector.detect(
      df,
      windowDuration = "10 minutes",
      slideDuration = "10 minutes",
      zThreshold = 3.0,
      minSamples = 10
    ).collect()

    result should have length 1
    val row = result.head
    row.getAs[Long]("sample_count") shouldBe 5L
    // it's the sample-count gate suppressing this, not the z-score math - confirm by
    // checking the same shape of outlier *does* get flagged once minSamples is met
    // (see the "flags an obvious outlier" test above)
    row.getAs[Boolean]("is_anomaly") shouldBe false
  }

  test("does not flag a stable window with no real outlier") {
    import spark.implicits._
    val df = readings(count = 30, outlierTemp = 20.0)
      .toDF("event_time", "chip_id", "temperature_celsius", "pressure")

    val result = AnomalyDetector.detect(
      df,
      windowDuration = "10 minutes",
      slideDuration = "10 minutes",
      zThreshold = 3.0,
      minSamples = 10
    ).collect()

    result should have length 1
    result.head.getAs[Boolean]("is_anomaly") shouldBe false
  }
}
