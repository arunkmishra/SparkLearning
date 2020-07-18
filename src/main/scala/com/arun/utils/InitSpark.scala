package com.arun.utils
import com.arun.joins.config.Config
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrameReader, SQLContext, SparkSession}

import scala.io.Source

trait InitSpark {

  val spark: SparkSession = SparkSession
    .builder()
    .appName("Learn Spark")
    .master("local[*]")
    .getOrCreate()

  def getSparkSession(appName: String = "Spark Application"): SparkSession = {
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName(appName)
      .getOrCreate()

    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("parquet.enable.dictionary", "false")
    spark.conf.set("spark.default.parallelism", Config.numberOfPartitions)
    spark.conf.set("spark.sql.shuffle.partitions", Config.numberOfPartitions)

    // Tell Spark to don't be too chatty
    spark.sparkContext.setLogLevel("WARN")

    spark
  }

  val sc: SparkContext = spark.sparkContext

  val sqlContext: SQLContext = spark.sqlContext

  val reader: DataFrameReader = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .option("mode", "DROPMALFORMED")

  def initLog = {
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.INFO)
    Logger.getLogger("akka").setLevel(Level.INFO)
    Logger.getRootLogger.setLevel(Level.INFO)
  }

  def loadFileFromResource(path: String) =
    reader.csv(getClass.getResource(path).getPath)
  initLog

  def sparkClose =
    spark.close()


}
