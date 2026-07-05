package com.gastongg.iot.sink

import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergSink {

  def ensureBronzeTable(spark: SparkSession, catalog: String): Unit = {
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.bronze")
    spark.sql(
      s"""
        |CREATE TABLE IF NOT EXISTS $catalog.bronze.sensor_readings (
        |  event_time TIMESTAMP,
        |  chip_id INT,
        |  altitude DOUBLE,
        |  pressure DOUBLE,
        |  temperature_celsius DOUBLE,
        |  temperature_fahrenheit DOUBLE,
        |  city STRING,
        |  country STRING,
        |  raw_value STRING,
        |  event_date DATE
        |) USING iceberg
        |PARTITIONED BY (event_date)
        |""".stripMargin
    )
  }

  def ensureAnomalyTable(spark: SparkSession, catalog: String): Unit = {
    spark.sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.silver")
    spark.sql(
      s"""
        |CREATE TABLE IF NOT EXISTS $catalog.silver.anomaly_windows (
        |  window_start TIMESTAMP,
        |  window_end TIMESTAMP,
        |  chip_id INT,
        |  sample_count LONG,
        |  mean_temp_c DOUBLE,
        |  stddev_temp_c DOUBLE,
        |  last_temp_c DOUBLE,
        |  temp_z_score DOUBLE,
        |  mean_pressure DOUBLE,
        |  stddev_pressure DOUBLE,
        |  last_pressure DOUBLE,
        |  pressure_z_score DOUBLE,
        |  is_anomaly BOOLEAN
        |) USING iceberg
        |PARTITIONED BY (days(window_start))
        |""".stripMargin
    )
  }

  /** Bronze is a single plain append sink - no fan-out needed. */
  def writeBronzeStream(df: DataFrame, catalog: String, checkpointLocation: String, triggerInterval: String): StreamingQuery = {
    df.withColumn("event_date", to_date(col("event_time")))
      .writeStream
      .format("iceberg")
      .outputMode("append")
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .toTable(s"$catalog.bronze.sensor_readings")
  }

  /** Used from within foreachBatch to append one micro-batch to the anomaly table. */
  def appendBatch(df: DataFrame, catalog: String): Unit = {
    df.writeTo(s"$catalog.silver.anomaly_windows").append()
  }
}
