package com.gastongg.iot

import org.apache.spark.sql.SparkSession

object SparkSessionFactory {

  def build(): SparkSession = {
    val builder = SparkSession.builder()
      .appName(AppConfig.appName)
      .master(AppConfig.sparkMaster)

    (AppConfig.getSparkConfigs ++ AppConfig.getIcebergCatalogConfigs)
      .foreach { case (key, value) => builder.config(key, value) }

    builder.getOrCreate()
  }
}
