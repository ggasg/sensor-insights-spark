package com.gaston.iot

import org.apache.spark.sql.SparkSession

object SensorReadingsApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName(AppConfig.appName)
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.hadoop.fs.s3a.endpoint", AppConfig.minioEndpoint)
      .config("spark.hadoop.fs.s3a.access.key", AppConfig.minioAccessKey)
      .config("spark.hadoop.fs.s3a.secret.key", AppConfig.minioSecretKey)
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val jobProcessing = new JobProcessing(spark)

    // Start bronze ingestion
    val rawData = jobProcessing.ingestFromKafka()
    val bronzeQuery = jobProcessing.saveToDelta("sensor_readings_raw", rawData, AppConfig.destinationBucket)
    // Silver with schema Compliance
    val silverData = jobProcessing.processSilver(s"${AppConfig.destinationBucket}/sensor_readings_raw")
    val silverQuery = jobProcessing.saveToDelta("sensor_readings_silver", silverData, AppConfig.destinationBucket)
    // Gold Averages
    val goldAggs = jobProcessing.processAverages(s"${AppConfig.destinationBucket}/sensor_readings_silver")
    val gold_aggs_query = jobProcessing.saveToDelta("sensor_readings_aggregates", goldAggs, AppConfig.destinationBucket, "complete")

    // Wait for all queries
    spark.streams.awaitAnyTermination()
  }
}
