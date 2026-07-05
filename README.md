# sensor-insights-spark

Spark Structured Streaming (Scala) side of a project comparing two approaches to
real-time anomaly detection on sensor data — this repo vs. a companion Materialize
SQL implementation (separate, not yet built) — on calculation accuracy, performance,
and maintainability (CI/CD, operability, etc).

Data source: a BMP180 temperature/pressure sensor on a Raspberry Pi
(producer code: `sensor-data-producer`, a separate repo), publishing over MQTT+TLS
directly into a Confluent Cloud Kafka topic (Confluent's native MQTT ingestion, no
bridge process). This job consumes that topic as a plain Kafka source.

## Pipeline (this phase)

```
Kafka topic (raw MQTT-mirrored JSON)
  -> parse + validate (SensorParser)
  -> bronze: typed rows -> Iceberg table on MinIO (bronze.sensor_readings)
  -> windowed rolling z-score anomaly detection (AnomalyDetector)
  -> silver: flagged windows -> Iceberg table (silver.anomaly_windows)
                              -> Kafka anomalies topic (for online consumers / a future dashboard)
```

Anomaly detection is deliberately expressed as a SQL-style windowed aggregation
(`groupBy(window(...), chip_id)` + z-score), not custom stateful Scala, so the exact
same query can later be run against Materialize SQL for a fair comparison.

Not in this repo yet: Materialize SQL implementation, Kubernetes deployment,
real-time dashboard.

## Prerequisites

* JDK 21 to properly run Spark 4.1.2 (see `build.sbt`)
* A Kafka topic receiving the sensor readings (e.g. Confluent Cloud, with the raw
  topic mirrored from MQTT as described above)
* MinIO (or any S3-compatible store) for the Iceberg warehouse

## Configuration

Project expects `app.conf` under `src/main/resources` (gitignored — fill in your
own values, or set the corresponding env vars):

```properties
kafka {
  bootstrap.servers = "<your_kafka_bootstraps>"
  bootstrap.servers = ${?KAFKA_BOOTSTRAP_SERVERS}
  topic = "<topic_with_raw_data>"
  anomalies-topic = "<topic_for_flagged_anomalies>"
  anomalies-topic = ${?KAFKA_ANOMALIES_TOPIC}

  # Confluent Cloud Kafka client auth - separate from the producer's MQTT broker credentials
  security {
    protocol = "SASL_SSL"
    protocol = ${?KAFKA_SECURITY_PROTOCOL}
    sasl-mechanism = "PLAIN"
    sasl-mechanism = ${?KAFKA_SASL_MECHANISM}
    api-key = "<confluent_kafka_api_key>"
    api-key = ${?KAFKA_API_KEY}
    api-secret = "<confluent_kafka_api_secret>"
    api-secret = ${?KAFKA_API_SECRET}
  }
}

iceberg {
  catalog-name = "local"
  catalog-name = ${?ICEBERG_CATALOG_NAME}
  warehouse-path = "s3a://<top_level_bucket_name>/warehouse"
  warehouse-path = ${?ICEBERG_WAREHOUSE_PATH}
}

anomaly {
  window-duration = "2 minutes"
  window-duration = ${?ANOMALY_WINDOW_DURATION}
  slide-duration = "30 seconds"
  slide-duration = ${?ANOMALY_SLIDE_DURATION}
  z-threshold = 3.0
  z-threshold = ${?ANOMALY_Z_THRESHOLD}
  min-samples = 30
  min-samples = ${?ANOMALY_MIN_SAMPLES}
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

  # Micro-batch interval. The sensor emits every ~5s; batching this avoids excessive
  # small-file creation on Iceberg (worth watching - this is one of the "Iceberg
  # performance drawback" angles this project wants to surface).
  trigger-interval = "30 seconds"
  trigger-interval = ${?SPARK_TRIGGER_INTERVAL}

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

The Iceberg catalog itself (`spark.sql.catalog.<name>`, extensions, warehouse path)
is wired up automatically from the `iceberg.*` block above — you don't need to
repeat it under `spark.configs`.

## Running

```
sbt test    # unit tests: JSON parsing (incl. the malformed MQTT last-will message) and anomaly z-score logic
sbt run     # starts the streaming job against your configured Kafka + MinIO
```

On startup the job creates the Iceberg namespaces/tables (`bronze`, `silver`) if
they don't already exist. To verify data is flowing end to end, query the tables
from a `spark-shell`/`sbt console` session, e.g.:

```scala
spark.sql("SELECT * FROM local.bronze.sensor_readings ORDER BY event_time DESC LIMIT 10").show()
spark.sql("SELECT * FROM local.silver.anomaly_windows WHERE is_anomaly ORDER BY window_start DESC LIMIT 10").show()
```
