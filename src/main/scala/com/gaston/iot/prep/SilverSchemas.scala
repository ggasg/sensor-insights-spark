package com.gaston.iot.prep

import org.apache.spark.sql.types._

object SilverSchemas {

  val locationSchema: StructType = StructType(Seq(
    StructField("ip", StringType, nullable = true),
    StructField("hostname", StringType, nullable = true),
    StructField("city", StringType, nullable = true),
    StructField("region", StringType, nullable = true),
    StructField("country", StringType, nullable = true),
    StructField("loc", StringType, nullable = true),
    StructField("org", StringType, nullable = true),
    StructField("postal", StringType, nullable = true),
    StructField("timezone", StringType, nullable = true),
    StructField("readme", StringType, nullable = true)
  ))

  val readingSchema: StructType = StructType(Seq(
    StructField("chip_id", IntegerType, nullable = true),
    StructField("altitude", LongType, nullable = true),
    StructField("pressure", LongType, nullable = true),
    StructField("temperature", ArrayType(LongType), nullable = true),
    StructField("location", StringType, nullable = true)
  ))

  val payloadSchema: StructType = StructType(Seq(
    StructField("timestamp", StringType, nullable = true),
    StructField("reading", StringType, nullable = true)
  ))
}