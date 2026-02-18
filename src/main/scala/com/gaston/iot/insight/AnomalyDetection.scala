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
  /**
   * Given a DF with the entire population of measurements, flag those readings with a temperature
   * change rate greater than 3.0 degrees, or pressure greater than 15.0. Also account for physically invalid values,
   * as well as negative values outside of the sensor's capabilities.
   * @param df Input DataFrame
   * @return DataFrame with records flagged as anomaly = true or false
   */
  private[iot] def detectAnomaliesRateChange(df: DataFrame): DataFrame = {
    val sensorWindow = Window.partitionBy("chip_id").orderBy("event_timestamp")

    val features = df
      .withColumn("temp_c", element_at(col("temperature"), lit(1)))
      .withColumn("temp_f", element_at(col("temperature"), lit(2)))
      .withColumn("prev_temp_c", lag("temp_c", 1).over(sensorWindow))
      .withColumn("temp_change_rate", col("temp_c") - col("prev_temp_c"))
      .withColumn("pressure_change_rate", col("pressure") - lag("pressure", 1).over(sensorWindow))
      .na.fill(0.0)

    features
      .withColumn("is_anomaly",
        when(
          // Sudden jumps compared with previous reading (sensor malfunction)
          (abs(col("temp_change_rate")) > 3.0) ||
            (abs(col("pressure_change_rate")) > 15.0) ||
            // Sustained anomalies (outside normal range)
            (col("temp_c") < 18.0) || (col("temp_c") > 23.0) ||
            (col("pressure") < 1005.0) || (col("pressure") > 1018.0) ||
            // Impossible values
            (col("temp_c") < -50.0) || (col("temp_c") > 100.0) ||
            (col("pressure") < 800.0) || (col("pressure") > 1200.0) ||
            (col("temp_c") === 0.0) || (col("pressure") === 0.0) ||
            // Negative values
            (col("temp_c") < 0.0) || (col("pressure") < 0.0),
          lit(true)
        ).otherwise(lit(false))
      )
  }

  /** Set up the Anomaly Detection Streaming Query. Focus on current batch only
   * @param spark Just your Spark Session since this is a top level method for your Spark Job
   * @param silverTable Source Table Name (e.g. "sensor_readings_silver")
   * @param outputTable Output Table Name (e.g. "sensor_anomalies")
   * @return Streaming Query to be handled in your Spark Job
   */
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

        // TODO --- Not in use since we're focusing on the current batch
        // Mark new batch records
        val batchMarked = batchDF.withColumn("is_new_batch", lit(true))
        val recentData = spark.read
          .format("delta")
          .load(silverTable)
          .filter(col("event_timestamp") > current_timestamp() - expr("INTERVAL 6 HOURS"))
          .limit(1000)
          .withColumn("is_new_batch", lit(false))

        val combinedData = recentData.union(batchMarked)
        // -- End Not in use

        val allResults = detectAnomaliesRateChange(batchDF)

        // Filter to only new batch records that are anomalies
        val anomalies = allResults
//          .filter(col("is_new_batch") === true)
          .filter(col("is_anomaly") === true)

        val anomalyCount = anomalies.count()

        if (logger.isDebugEnabled()) {
          allResults.cache()
          logger.debug(s"Total anomalies detected: ${allResults.filter(col("is_anomaly") === true).count()}")
          logger.debug(s"New anomalies to save: ${allResults.filter(col("is_new_batch") === true && col("is_anomaly") === true).count()}")
          allResults.unpersist()
        }

        // Save to Delta if anomalies found
        if (anomalyCount > 0) {
          anomalies
//            .drop("is_new_batch")
            .write
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
