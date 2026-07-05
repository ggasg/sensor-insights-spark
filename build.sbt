scalaVersion := "2.13.17"

lazy val root = rootProject
  .settings(
    name := "sensor-insights-spark",
  )

val sparkVersion = "4.1.2"
val hadoopVersion = "3.5.0"
val icebergVersion = "1.11.0"
val log4jVersion = "2.26.1"
val awsSdkVersion = "1.12.797"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4jVersion,
  // Iceberg
   "org.apache.iceberg" %% "iceberg-spark-runtime-4.1" % icebergVersion,
  // streaming-kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  // S3 support
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk-bundle" % awsSdkVersion,
  // Typesafe for app config
  "com.typesafe" % "config" % "1.4.9",
  // Tests
  "org.scalatest" %% "scalatest" % "3.2.19" % Test
)