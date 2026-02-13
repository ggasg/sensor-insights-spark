package com.gaston.iot.config

import com.typesafe.config.ConfigFactory

import scala.jdk.CollectionConverters._

object AppConfig {

  private val config = ConfigFactory.load("app")

  // Kafka
  val kafkaBootstrap: String = config.getString("kafka.bootstrap.servers")
  val kafkaTopic: String = config.getString("kafka.topic")

  // MinIO
  val minioEndpoint: String = config.getString("minio.endpoint")
  val minioAccessKey: String = config.getString("minio.access-key")
  val minioSecretKey: String = config.getString("minio.secret-key")
  val destinationBucket: String = config.getString("minio.destination-bucket")

  // Spark
  val appName: String = config.getString("spark.app-name")
  val sparkMaster: String = config.getString("spark.master")
  val checkpointLocation: String = config.getString("spark.checkpoint-location")

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
}
