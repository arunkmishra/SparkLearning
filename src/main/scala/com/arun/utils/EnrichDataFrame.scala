package com.arun.utils
import org.apache.spark.rdd.RDD
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

    def getDataPerPartition: RDD[Int] =
      df.rdd.mapPartitionsWithIndex { (partition, row) =>
        println(s"Partition[$partition] -> ${row.toList.map(i =>i.get(1))}")
        Iterator(partition, row.size)
      }
  }
}
