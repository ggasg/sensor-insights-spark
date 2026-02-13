# sensor-insights-spark
Processing Temperature measures from home raspberry pi with temperature & Pressure sensor attached.

Prerequisites:

* JDK 17 to properly run on Spark 4.0.0 (see build.sbt)
* Connection to a working Kafka Cluster (e.g. Confluent "all in one")
* Minio for S3-compatible storage api

Project expects app.conf under src/main/resources with the following contents:

```properties
kafka {
  bootstrap.servers = "<your_kafka_bootstraps>"
  bootstrap.servers = ${?KAFKA_BOOTSTRAP_SERVERS}
  topic = "<topic_with_raw_data>"
}

minio {
  endpoint = "<minio_endpoint>"
  endpoint = ${?MINIO_ENDPOINT}
  access-key = "<minio_user>"
  access-key = ${?MINIO_ACCESS_KEY}
  secret-key = "<minio_key>"
  secret-key = ${?MINIO_SECRET_KEY}
  destination-bucket = "<top_level_bucket_name>"
  destination-bucket = ${?DEST_BUCKET_URL}
}

spark {
  app-name = "<your_favorite_app_name>"
  master = "local[*]"
  master = ${?SPARK_MASTER}

  checkpoint-location = "<use bucket/_checkpoint>"
  checkpoint-location = ${?SPARK_CHECKPOINT_LOCATION}

  configs {
    spark.hadoop.fs.s3a.endpoint = "<minio_endpoint>"
    spark.hadoop.fs.s3a.endpoint = ${?MINIO_ENDPOINT}
    spark.hadoop.fs.s3a.access.key = "<minio_user>"
    spark.hadoop.fs.s3a.access.key = ${?MINIO_ACCESS_KEY}
    spark.hadoop.fs.s3a.secret.key = "<minio_key>"
    spark.hadoop.fs.s3a.secret.key = ${?MINIO_SECRET_KEY}
    spark.hadoop.fs.s3a.path.style.access = "true"
    spark.hadoop.fs.s3a.impl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
  }
}
```