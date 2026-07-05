package com.gastongg.iot.anomaly

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

/**
 * Windowed rolling z-score anomaly detection, expressed as a plain SQL-style
 * aggregation (`groupBy(window(...), chip_id)`) rather than custom stateful Scala
 * (e.g. `mapGroupsWithState`). This is deliberate: the point of this project is to
 * compare Spark against Materialize SQL on the *same* query, and a windowed
 * aggregation is the form both engines can express natively (materialized view /
 * `OVER` window in Materialize).
 *
 * Written as a plain `DataFrame => DataFrame` transform so the exact same logic
 * runs, and is unit-tested, in batch mode - Structured Streaming just adds a
 * watermark and a streaming source on top of it.
 */
object AnomalyDetector {

  def detect(
    readings: DataFrame,
    windowDuration: String,
    slideDuration: String,
    zThreshold: Double,
    minSamples: Long,
    watermarkDelay: String = "1 minute"
  ): DataFrame = {

    val stats = readings
      .withWatermark("event_time", watermarkDelay)
      .groupBy(window(col("event_time"), windowDuration, slideDuration).as("window"), col("chip_id"))
      .agg(
        count(lit(1)).as("sample_count"),
        avg(col("temperature_celsius")).as("mean_temp_c"),
        stddev(col("temperature_celsius")).as("stddev_temp_c"),
        max_by(col("temperature_celsius"), col("event_time")).as("last_temp_c"),
        avg(col("pressure")).as("mean_pressure"),
        stddev(col("pressure")).as("stddev_pressure"),
        max_by(col("pressure"), col("event_time")).as("last_pressure")
      )

    val tempZScore = zScore(col("last_temp_c"), col("mean_temp_c"), col("stddev_temp_c"))
    val pressureZScore = zScore(col("last_pressure"), col("mean_pressure"), col("stddev_pressure"))

    stats
      .withColumn("window_start", col("window.start"))
      .withColumn("window_end", col("window.end"))
      .withColumn("temp_z_score", tempZScore)
      .withColumn("pressure_z_score", pressureZScore)
      .withColumn(
        "is_anomaly",
        col("sample_count") >= minSamples &&
          (abs(col("temp_z_score")) > zThreshold || abs(col("pressure_z_score")) > zThreshold)
      )
      .drop("window")
  }

  /** Null/zero stddev (e.g. a window with a single distinct value) can't produce a meaningful z-score. */
  private def zScore(value: Column, mean: Column, stddev: Column): Column =
    when(stddev > 0, (value - mean) / stddev).otherwise(lit(0.0))
}
