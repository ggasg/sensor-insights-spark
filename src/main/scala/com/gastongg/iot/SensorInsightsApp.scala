package com.gastongg.iot

import com.gastongg.iot.anomaly.AnomalyDetector
import com.gastongg.iot.ingestion.{KafkaReader, SensorParser}
import com.gastongg.iot.sink.{IcebergSink, KafkaAnomalySink}
import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

object SensorInsightsApp {

  private val logger = LogManager.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.build()
    spark.sparkContext.setLogLevel("WARN")

    IcebergSink.ensureBronzeTable(spark, AppConfig.icebergCatalogName)
    IcebergSink.ensureAnomalyTable(spark, AppConfig.icebergCatalogName)

    val rawStream = KafkaReader.readStream(spark, AppConfig.kafkaTopic)
    val parsed = SensorParser.parse(rawStream)
    val corrupt = SensorParser.corruptRecords(rawStream)

    IcebergSink.writeBronzeStream(
      parsed,
      AppConfig.icebergCatalogName,
      AppConfig.bronzeCheckpointLocation,
      AppConfig.triggerInterval
    )

    val anomalyWindows = AnomalyDetector.detect(
      parsed,
      AppConfig.anomalyWindowDuration,
      AppConfig.anomalySlideDuration,
      AppConfig.anomalyZThreshold,
      AppConfig.anomalyMinSamples
    )

    anomalyWindows.writeStream
      .outputMode("append")
      .option("checkpointLocation", AppConfig.anomalyCheckpointLocation)
      .trigger(Trigger.ProcessingTime(AppConfig.triggerInterval))
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        batchDf.persist()
        IcebergSink.appendBatch(batchDf, AppConfig.icebergCatalogName)
        KafkaAnomalySink.writeBatch(batchDf, AppConfig.anomaliesTopic)
        batchDf.unpersist()
        ()
      }
      .start()

    corrupt.writeStream
      .outputMode("append")
      .option("checkpointLocation", AppConfig.corruptCheckpointLocation)
      .trigger(Trigger.ProcessingTime(AppConfig.triggerInterval))
      .foreachBatch { (batchDf: DataFrame, _: Long) =>
        val count = batchDf.count()
        if (count > 0) logger.warn(s"Dropped $count message(s) that did not match the sensor reading schema")
        ()
      }
      .start()

    spark.streams.awaitAnyTermination()
  }
}
