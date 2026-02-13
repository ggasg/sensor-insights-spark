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

  private[insight] def detectAnomalies(df: DataFrame): DataFrame = {
    // Window with partitioning. Not really in use since there's only one chip id
    val windowSpec = Window
      .partitionBy("chip_id")
      .orderBy("event_timestamp")

    // Add features: lag and measurement diffs
    val features = df
      .withColumn("temp_c", element_at(col("temperature"), lit(1)))
      .withColumn("temp_f", element_at(col("temperature"), lit(1)))
      .withColumn("prev_temp_c", lag("temp_c", 1).over(windowSpec))
      .withColumn("temp_change_rate", col("temp_c") - col("prev_temp_c"))
      .withColumn("pressure_change_rate", col("pressure") - lag("pressure", 1).over(windowSpec))
      .na.fill(0.0)

    // Assemble
    val assembler = new VectorAssembler()
      .setInputCols(Array("temp_c", "pressure", "altitude", "temp_change_rate", "pressure_change_rate"))
      .setOutputCol("features")

    val featurized = assembler.transform(features)

    // Anomaly detection via just plain old stats
    val stats = featurized.select(
      mean("temp_c").alias("mean_temp"),
      stddev("temp_c").alias("stddev_ temp"),
      mean("pressure").alias("mean_pressure"),
      stddev("pressure").alias("stddev_pressure")
    ).first()

    // Smaller batches will have identical values. Standard Dev = 0 so need to avoid div by 0
    val stddevTemp = if (stats.getDouble(1) == 0.0) 1.0 else stats.getDouble(1)
    val stddevPressure = if (stats.getDouble(3) == 0.0) 1.0 else stats.getDouble(3)


    // Flag values > 3 std devs
    featurized
      .withColumn("temp_zscore", abs((col("temp_c") - lit(stats.getDouble(0))) / lit(stddevTemp)))
      .withColumn("pressure_zscore", abs((col("pressure") - lit(stats.getDouble(2))) / lit(stddevPressure)))
      .withColumn("is_anomaly",
        when(col("temp_zscore") > 3.0 || col("pressure_zscore") > 3.0, lit(true))
          .otherwise(lit(false))
      )
  }

  def startAnomalyDetectionStream(
                                 spark: SparkSession,
                                 silverTable: String,
                                 outputTable: String,
                                 ): StreamingQuery = {
    logger.info("Starting anomaly detection stream")

    spark.readStream
      .format("delta")
      .load(silverTable)
      .writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        logger.info(s"Processing batch $batchId for anomaly detection")

        logger.debug(s"Batch columns: ${batchDF.columns.mkString(", ")}")

        val recentData = spark.read
          .format("delta")
          .load(silverTable)
          .filter(col("event_timestamp") > current_timestamp() - expr("INTERVAL 6 HOURS"))
          .limit(1000)

        val combinedData = recentData.union(batchDF)
        val anomalies = detectAnomalies(combinedData)
          .filter(col("is_anomaly") === true)

        anomalies.write
            .format("delta")
            .mode("append")
            .save(outputTable)
      }
      .option("checkpointLocation", s"${AppConfig.checkpointLocation}/anomalies")
      .start()
  }
}
