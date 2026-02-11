package com.gaston.iot

import com.typesafe.config.ConfigFactory

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
  val checkpointLocation: String = config.getString("spark.checkpoint-location")

  def printConfig(): Unit = {
    println(s"Kafka Bootstrap: $kafkaBootstrap")
    println(s"Source: ${if (sys.env.contains("KAFKA_BOOTSTRAP_SERVERS")) "ENV VAR" else "application.conf"}")
  }
}
