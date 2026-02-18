package com.gaston.iot.insight

import com.gaston.iot.config.AppConfig
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery
import org.slf4j.LoggerFactory

object AnomalyDetection {

  private val logger = LoggerFactory.getLogger(getClass)

  private[iot] def detectAnomalies(df: DataFrame): DataFrame = {
    val sensorWindow = Window.partitionBy("chip_id").orderBy("event_timestamp")

    val features = df
      .withColumn("temp_c", element_at(col("temperature"), lit(1)))
      .withColumn("temp_f", element_at(col("temperature"), lit(2)))
      .withColumn("prev_temp_c", lag("temp_c", 1).over(sensorWindow))
      .withColumn("temp_change_rate", col("temp_c") - col("prev_temp_c"))
      .withColumn("pressure_change_rate", col("pressure") - lag("pressure", 1).over(sensorWindow))
      .na.fill(0.0)

    // Anomaly detection based on thresholds and rate of change
    features
      .withColumn("is_anomaly",
        when(
          // Sudden jumps
          (abs(col("temp_change_rate")) > 3.0) ||
            (abs(col("pressure_change_rate")) > 15.0) ||
            // Invalid values
            (col("temp_c") < -50.0) || (col("temp_c") > 100.0) ||
            (col("pressure") < 800.0) || (col("pressure") > 1200.0) ||
            (col("temp_c") === 0.0) || (col("pressure") === 0.0) ||
            // Negative values
            (col("temp_c") < 0.0) || (col("pressure") < 0.0),
          lit(true)
        ).otherwise(lit(false))
      )
  }

  def startAnomalyDetectionStream(
                                   spark: SparkSession,
                                   silverTable: String,
                                   outputTable: String
                                 ): StreamingQuery = {

    logger.info("Starting anomaly detection stream")

    spark.readStream
      .format("delta")
      .load(silverTable)
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Processing batch $batchId for anomaly detection")

        // Mark new batch records
        val batchMarked = batchDF.withColumn("is_new_batch", lit(true))

        // Read recent history for better context
        val recentData = spark.read
          .format("delta")
          .load(silverTable)
          .filter(col("event_timestamp") > current_timestamp() - expr("INTERVAL 6 HOURS"))
          .limit(1000)
          .withColumn("is_new_batch", lit(false))

        // Combine for detection
        val combinedData = recentData.union(batchMarked)
        val allResults = detectAnomalies(combinedData)

        // Filter to only new batch records that are anomalies
        val anomalies = allResults
          .filter(col("is_new_batch") === true)
          .filter(col("is_anomaly") === true)
          .drop("is_new_batch")

        val anomalyCount = anomalies.count()
        logger.info(s"Batch $batchId: Found $anomalyCount anomalies in new data")

        // Save to Delta if anomalies found
        if (anomalyCount > 0) {
          anomalies.write
            .format("delta")
            .mode("append")
            .save(outputTable)
        }
      }
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", s"${AppConfig.checkpointLocation}/anomalies")
      .start()
  }
}
