package com.gastongg.iot

import org.apache.spark.sql.SparkSession

/**
 * Spark only allows one active SparkSession/SparkContext per JVM by default, so
 * every spec in this test run shares this one instance rather than each spec
 * creating (and stopping) its own.
 */
object SparkTestSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .appName("sensor-insights-spark-tests")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
}
