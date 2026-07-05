package com.gastongg.iot.ingestion

import com.gastongg.iot.model.SensorSchemas
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StringType, TimestampType}
import org.apache.spark.sql.{DataFrame, Column}

/**
 * Parses raw Kafka `value` bytes into the typed sensor reading schema. Handles the
 * fact that not every message on the topic conforms to the reading schema (e.g. the
 * MQTT producer's last-will-on-disconnect message, which isn't even valid JSON) by
 * routing anything that doesn't parse cleanly to `corruptRecords` instead of failing
 * the query.
 */
object SensorParser {

  private val schemaWithCorrupt =
    SensorSchemas.rawMessageSchema.add("_corrupt_record", StringType, nullable = true)

  private def withParsedJson(raw: DataFrame): DataFrame = {
    raw
      .select(col("value").cast(StringType).as("raw_value"))
      .withColumn(
        "parsed",
        from_json(col("raw_value"), schemaWithCorrupt, Map("columnNameOfCorruptRecord" -> "_corrupt_record"))
      )
  }

  private def isValid: Column =
    col("parsed._corrupt_record").isNull && col("parsed.reading").isNotNull

  /** Epoch-nanoseconds string -> Timestamp (Spark casts numeric->timestamp as seconds since epoch). */
  private def eventTimeFromNanos(nanosCol: Column): Column =
    (nanosCol.cast(DecimalType(30, 0)) / 1e9).cast(TimestampType)

  /** Valid, flattened rows matching the SensorReading case class shape. */
  def parse(raw: DataFrame): DataFrame = {
    withParsedJson(raw)
      .filter(isValid)
      .select(
        eventTimeFromNanos(col("parsed.timestamp")).as("event_time"),
        col("parsed.reading.chip_id").as("chip_id"),
        col("parsed.reading.altitude").as("altitude"),
        col("parsed.reading.pressure").as("pressure"),
        element_at(col("parsed.reading.temperature"), 1).as("temperature_celsius"),
        element_at(col("parsed.reading.temperature"), 2).as("temperature_fahrenheit"),
        col("parsed.reading.location.city").as("city"),
        col("parsed.reading.location.country").as("country"),
        col("raw_value")
      )
  }

  /** Rows that failed to parse as a reading, kept for observability/dead-lettering instead of being silently dropped. */
  def corruptRecords(raw: DataFrame): DataFrame = {
    withParsedJson(raw)
      .filter(!isValid)
      .select(
        col("raw_value"),
        coalesce(col("parsed._corrupt_record"), lit("missing `reading` field")).as("reason")
      )
  }
}
