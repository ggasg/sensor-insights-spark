package com.gastongg.iot

import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._

object AppConfig {
  private val config = ConfigFactory.load("app")

  // Kafka
  val kafkaBootstrap: String = config.getString("kafka.bootstrap.servers")
  val kafkaTopic: String = config.getString("kafka.topic")
  val anomaliesTopic: String = config.getString("kafka.anomalies-topic")

  // Kafka client security (Confluent Cloud SASL_SSL, separate from the producer's MQTT credentials)
  val kafkaSecurityProtocol: String = config.getString("kafka.security.protocol")
  val kafkaSaslMechanism: String = config.getString("kafka.security.sasl-mechanism")
  val kafkaApiKey: String = config.getString("kafka.security.api-key")
  val kafkaApiSecret: String = config.getString("kafka.security.api-secret")

  def kafkaSaslJaasConfig: String =
    s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$kafkaApiKey" password="$kafkaApiSecret";"""

  // MinIO
  val minioEndpoint: String = config.getString("minio.endpoint")
  val minioAccessKey: String = config.getString("minio.access-key")
  val minioSecretKey: String = config.getString("minio.secret-key")
  val destinationBucket: String = config.getString("minio.destination-bucket")

  // Iceberg
  val icebergCatalogName: String = config.getString("iceberg.catalog-name")
  val icebergWarehousePath: String = config.getString("iceberg.warehouse-path")

  // Anomaly detection
  val anomalyWindowDuration: String = config.getString("anomaly.window-duration")
  val anomalySlideDuration: String = config.getString("anomaly.slide-duration")
  val anomalyZThreshold: Double = config.getDouble("anomaly.z-threshold")
  val anomalyMinSamples: Long = config.getLong("anomaly.min-samples")

  // Spark
  val appName: String = config.getString("spark.app-name")
  val sparkMaster: String = config.getString("spark.master")
  val checkpointLocation: String = config.getString("spark.checkpoint-location")
  val triggerInterval: String = config.getString("spark.trigger-interval")

  val bronzeCheckpointLocation: String = s"$checkpointLocation/bronze"
  val anomalyCheckpointLocation: String = s"$checkpointLocation/anomaly"
  val corruptCheckpointLocation: String = s"$checkpointLocation/corrupt"

  // Configs
  def getSparkConfigs: Map[String, String] = {
    config.getConfig("spark.configs")
      .entrySet()
      .asScala
      .map { entry =>
        val key = entry.getKey.replace("\"", "")
        val value = entry.getValue.unwrapped().toString
        key -> value
      }
      .toMap
  }

  def getIcebergCatalogConfigs: Map[String, String] = Map(
    s"spark.sql.catalog.$icebergCatalogName" -> "org.apache.iceberg.spark.SparkCatalog",
    s"spark.sql.catalog.$icebergCatalogName.type" -> "hadoop",
    s"spark.sql.catalog.$icebergCatalogName.warehouse" -> icebergWarehousePath,
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  )
}
