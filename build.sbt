name := "SparkLearning"

version := "0.1"

scalaVersion := "2.12.4"

val sparkVersion = "2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.1.1" % "test",
  "com.typesafe" % "config" % "1.3.1"
)
