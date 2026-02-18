package com.gaston.iot

import com.gaston.iot.config.AppConfig
import com.gaston.iot.insight.AnomalyDetection
import com.gaston.iot.prep.JobProcessing
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SensorReadingsApp {

  def main(args: Array[String]): Unit = {

    val builder = SparkSession
      .builder()
      .appName(AppConfig.appName)
      .master(AppConfig.sparkMaster)
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    // Get Spark Configuration props from AppConfig
    AppConfig.getSparkConfigs.foreach { case(key, value) =>
      builder.config(key, value)
    }
    val spark = builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    val logger = LoggerFactory.getLogger(getClass)

    val jobProcessing = new JobProcessing(spark)

    val rawData = jobProcessing.ingestFromKafka()
    val bronzeQuery = jobProcessing.saveToDelta("sensor_readings_raw", rawData, AppConfig.destinationBucket)

    // Silver with schema Compliance
    val silverData = jobProcessing.processSilver(s"${AppConfig.destinationBucket}/sensor_readings_raw")
    val silverQuery = jobProcessing.saveToDelta("sensor_readings_silver", silverData, AppConfig.destinationBucket)

    // Gold Averages
    val goldAggs = jobProcessing.processAverages(s"${AppConfig.destinationBucket}/sensor_readings_silver")
    val gold_aggs_query = jobProcessing.saveToDelta("sensor_readings_aggregates", goldAggs, AppConfig.destinationBucket, "complete")

    // Anomaly Detection - Account for up to 6 hours before + current micro batch
    val anomalyQuery = AnomalyDetection.startAnomalyDetectionStream(
      spark,
      silverTable = s"${AppConfig.destinationBucket}/sensor_readings_silver",
      outputTable = s"${AppConfig.destinationBucket}/sensor_readings_anomalies"
    )

    logger.debug("--- Awaiting for all Streaming Queries")
    spark.streams.awaitAnyTermination()
  }
}
