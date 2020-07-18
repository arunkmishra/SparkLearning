package com.arun.joins.join

import org.apache.spark.sql.{DataFrame, SparkSession}

object NormalJoin extends JoinStrategy {

  override def join(spark: SparkSession, dfLarge: DataFrame, dfMedium: DataFrame): DataFrame = {
    // Explicitly disable the broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    dfLarge
      .join(
        dfMedium,
        Seq("key"),
        "left_outer"
      )
      .select(
        dfLarge("key"),
        dfMedium("label")
      )
  }
}