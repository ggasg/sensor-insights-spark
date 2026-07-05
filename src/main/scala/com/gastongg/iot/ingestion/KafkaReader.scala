package com.gastongg.iot.ingestion

import com.gastongg.iot.AppConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaReader {

  /** Reads raw records (key/value bytes + Kafka metadata) from the given topic. */
  def readStream(spark: SparkSession, topic: String, startingOffsets: String = "latest"): DataFrame = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrap)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("kafka.security.protocol", AppConfig.kafkaSecurityProtocol)
      .option("kafka.sasl.mechanism", AppConfig.kafkaSaslMechanism)
      .option("kafka.sasl.jaas.config", AppConfig.kafkaSaslJaasConfig)
      // The producer's MQTT last-will message and any transient malformed records
      // shouldn't kill the query; SensorParser is responsible for filtering them out.
      .option("failOnDataLoss", "false")
      .load()
  }
}
