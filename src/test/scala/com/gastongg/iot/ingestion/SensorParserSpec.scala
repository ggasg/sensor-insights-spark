package com.gastongg.iot.ingestion

import com.gastongg.iot.SparkTestSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SensorParserSpec extends AnyFunSuite with Matchers {

  private val spark = SparkTestSession.spark

  // Mirrors the real producer payload shape (~/dev/sensor-data-producer/client_connect.py).
  private val validJson =
    """{"timestamp":"1700000000000000000","reading":{"chip_id":87,"altitude":123.4,"pressure":987.6,""" +
      """"temperature":[21.3,70.4],"location":{"ip":"1.2.3.4","city":"Springfield","region":"IL",""" +
      """"country":"US","loc":"1,2","org":"ISP","postal":"62701","timezone":"America/Chicago"}}}"""

  // The producer's MQTT last-will-on-disconnect payload: not valid JSON (no wrapping braces).
  private val lastWillMessage = """"timestamp":"1700000000000000000", "Client Disconnect""""

  test("parse extracts a valid reading with correctly flattened fields") {
    import spark.implicits._
    val raw = Seq(validJson).toDF("value")

    val rows = SensorParser.parse(raw).collect()

    rows should have length 1
    val row = rows.head
    row.getAs[Int]("chip_id") shouldBe 87
    row.getAs[Double]("altitude") shouldBe 123.4
    row.getAs[Double]("pressure") shouldBe 987.6
    row.getAs[Double]("temperature_celsius") shouldBe 21.3
    row.getAs[Double]("temperature_fahrenheit") shouldBe 70.4
    row.getAs[String]("city") shouldBe "Springfield"
    row.getAs[String]("country") shouldBe "US"

    val eventTime = row.getAs[java.sql.Timestamp]("event_time").toInstant
    val expected = java.time.Instant.ofEpochSecond(1700000000L)
    math.abs(java.time.Duration.between(expected, eventTime).toMillis) should be < 10L
  }

  test("parse drops messages that don't conform to the reading schema") {
    import spark.implicits._
    val raw = Seq(validJson, lastWillMessage).toDF("value")

    SensorParser.parse(raw).count() shouldBe 1
  }

  test("corruptRecords surfaces messages that don't match the reading schema instead of silently dropping them") {
    import spark.implicits._
    val raw = Seq(validJson, lastWillMessage).toDF("value")

    val corrupt = SensorParser.corruptRecords(raw).collect()
    corrupt should have length 1
    corrupt.head.getAs[String]("raw_value") shouldBe lastWillMessage
  }
}
