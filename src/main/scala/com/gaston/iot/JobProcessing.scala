package com.gaston.iot

import com.gaston.iot.SilverSchemas.{locationSchema, payloadSchema, readingSchema}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, element_at, expr, from_json, lit, to_timestamp, window}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.Trigger


class JobProcessing(sparkSession: SparkSession) {

  val KAFKA_OPTIONS: Map[String, String] = Map(
    "kafka.bootstrap.servers" -> AppConfig.kafkaBootstrap,
    "subscribe" -> AppConfig.kafkaTopic,
    "kafka.sasl.mechanism" -> "PLAIN"
  )

  private[iot] def enrichBronze(input_df: DataFrame): DataFrame = {
    input_df
      .withColumn("payload", col("value").cast("string"))
      .withColumn("mqtt_enqueued_timestamp", expr("timestamp"))
      .withColumn("ingress_timestamp", col("current_timestamp"))
      .withColumn("topic", lit(AppConfig.kafkaTopic))
      .drop("value", "key")
  }

  def ingestFromKafka(): DataFrame = {
    sparkSession.readStream
      .format("kafka")
      .options(KAFKA_OPTIONS)
      .load()
      .transform(enrichBronze)
  }

  def processSilver(bronzeTable: String): DataFrame = {
    sparkSession.readStream
      .format("delta")
      .load(bronzeTable)
      .withColumn("parsed_payload", from_json(col("payload"), payloadSchema))
      .withColumn("parsed_reading", from_json(col("parsed_payload.reading"), readingSchema))
      .withColumn("parsed_location", from_json(col("parsed_reading.location"), locationSchema))
      .select(
        col("parsed_payload.timestamp").alias("event_timestamp"),
        col("parsed_reading.chip_id"),
        col("parsed_reading.altitude"),
        col("parsed_reading.pressure"),
        col("parsed_reading.temperature"),
        col("parsed_location.*")  // Flatten location fields
      )
  }

  private[iot] def computeWindowAverages(df: DataFrame): DataFrame = {
    df.withColumn("ts", (col("event_timestamp").cast("long") / 1e9).cast("timestamp"))
      .groupBy(window(col("ts"), "5 minutes"))
      .agg(
        avg(element_at(col("temperature"), lit(1))).alias("avg_c"),
        avg(element_at(col("temperature"), lit(2))).alias("avg_f"),
        avg(col("altitude")).alias("avg_altitude"),
        avg(col("pressure")).alias("avg_pressure")
      )
      .select("window.start", "window.end", "avg_c", "avg_f", "avg_altitude", "avg_pressure")
  }

  // Get average values for altitude, pressure, and temperatures in a tumbling 5 minute window
  def processAverages(silverTable: String): DataFrame = {
    sparkSession.readStream
      .format("delta")
      .load(silverTable)
      .transform(computeWindowAverages)
  }

  def saveToDelta(tableName: String, rows: DataFrame, basePath: String = "tmp", outputMode: String = "append"): StreamingQuery = {
    if (rows.isStreaming) {
      rows.writeStream
        .format("delta")
        .outputMode(outputMode)
        .option("checkpointLocation", s"$basePath/_checkpoint/$tableName")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start(s"$basePath/$tableName")

    } else {
      throw new IllegalArgumentException("DataFrame must be streaming")
    }
  }
}
