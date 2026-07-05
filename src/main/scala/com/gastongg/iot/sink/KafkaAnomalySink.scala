package com.gastongg.iot.sink

import com.gastongg.iot.AppConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, struct, to_json}

/** Used from within foreachBatch to publish only the flagged windows for online consumption. */
object KafkaAnomalySink {

  def writeBatch(df: DataFrame, topic: String): Unit = {
    val anomalies = df.filter(col("is_anomaly"))

    anomalies
      .select(to_json(struct(anomalies.columns.map(col): _*)).as("value"))
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.kafkaBootstrap)
      .option("topic", topic)
      .option("kafka.security.protocol", AppConfig.kafkaSecurityProtocol)
      .option("kafka.sasl.mechanism", AppConfig.kafkaSaslMechanism)
      .option("kafka.sasl.jaas.config", AppConfig.kafkaSaslJaasConfig)
      .save()
  }
}
