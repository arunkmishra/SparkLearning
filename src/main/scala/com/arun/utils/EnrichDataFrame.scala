package com.arun.utils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object EnrichDataFrame {

  implicit class DataFrameEnrich(df: DataFrame) {
    import df.sparkSession.implicits._

    def debugString(): String =
      df.rdd.toDebugString

    def getNumPartition: Int =
      df.rdd.getNumPartitions

    def calculateSize(size: Size): Double =
      SizeCalculator.calculateSize(df, size)

    def getPartitionForRow: DataFrame =
      df.withColumn("partition", spark_partition_id())
  }
}
