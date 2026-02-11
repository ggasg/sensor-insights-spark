ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

lazy val root = (project in file("."))
  .settings(
    name := "sensor-insights-spark"
  )

val sparkVersion = "4.0.0"
val log4jVersion = "2.25.3"
val deltaLakeVersion = "4.0.0"
val hadoopVersion = "3.4.1"
val awsSdkVersion = "1.12.780"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  // Delta Lake
  "io.delta" %% "delta-spark" % deltaLakeVersion,
  // streaming-kafka
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  // S3 support
  "org.apache.hadoop" % "hadoop-aws" % hadoopVersion,
  "com.amazonaws" % "aws-java-sdk-bundle" % awsSdkVersion,
  // Typesafe for app config
  "com.typesafe" % "config" % "1.4.5"
)

// Unit Testing
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % Test,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Test,
  "io.delta" %% "delta-spark" % deltaLakeVersion % Test
)