package com.gastongg.iot.model

import org.apache.spark.sql.types._

/**
 * Schema for the JSON payload the Raspberry Pi producer publishes over MQTT into the
 * Confluent-mirrored Kafka topic (see ~/dev/sensor-data-producer/client_connect.py):
 *
 * {
 *   "timestamp": "1720100000123456789",
 *   "reading": {
 *     "chip_id": 87,
 *     "altitude": 123.4,
 *     "pressure": 987.6,
 *     "temperature": [21.3, 70.4],
 *     "location": { "ip": "...", "city": "...", "region": "...", "country": "...",
 *                   "loc": "lat,lon", "org": "...", "postal": "...", "timezone": "..." }
 *   }
 * }
 *
 * `timestamp` is epoch nanoseconds as a string. `temperature` is `[celsius, fahrenheit]`.
 * The MQTT last-will-on-disconnect message does not conform to this schema (it isn't
 * even valid JSON), so `reading` must stay nullable/optional end to end.
 */
object SensorSchemas {

  val locationSchema: StructType = StructType(Seq(
    StructField("ip", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("region", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("loc", StringType, nullable = true),
    StructField("org", StringType, nullable = true),
    StructField("postal", StringType, nullable = true),
    StructField("timezone", StringType, nullable = true)
  ))

  val readingSchema: StructType = StructType(Seq(
    StructField("chip_id", IntegerType, nullable = true),
    StructField("altitude", DoubleType, nullable = true),
    StructField("pressure", DoubleType, nullable = true),
    StructField("temperature", ArrayType(DoubleType), nullable = true),
    StructField("location", locationSchema, nullable = true)
  ))

  val rawMessageSchema: StructType = StructType(Seq(
    StructField("timestamp", StringType, nullable = true),
    StructField("reading", readingSchema, nullable = true)
  ))
}

case class Location(
  ip: Option[String],
  city: Option[String],
  region: Option[String],
  country: Option[String],
  loc: Option[String],
  org: Option[String],
  postal: Option[String],
  timezone: Option[String]
)

/** Flattened, typed row produced by SensorParser from a raw Kafka message. */
case class SensorReading(
  eventTime: java.sql.Timestamp,
  chipId: Int,
  altitude: Double,
  pressure: Double,
  temperatureCelsius: Double,
  temperatureFahrenheit: Double,
  city: Option[String],
  country: Option[String],
  rawValue: String
)
