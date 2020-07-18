package com.arun.utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.util.SizeEstimator

trait Size {
  def calculate(sizeInBytes: Long): Double
}

case object KiloByte extends Size {
  override def calculate(sizeInBytes: Long): Double =
    sizeInBytes / 1000
  override def toString: String = " MB"
}
case object MegaByte extends Size {
  override def calculate(sizeInBytes: Long): Double =
    sizeInBytes / (1000 * 1000)
  override def toString: String = " MB"
}
case object GigaByte extends Size {
  override def calculate(sizeInBytes: Long): Double =
    sizeInBytes / (1000 * 1000 * 1000)
  override def toString: String = " MB"
}

object SizeCalculator {

  def calcRddSize(rdd: RDD[String]): Long = {
    rdd.map(_.getBytes("UTF-8").length.toLong)
      .reduce(_ + _)
  }

  def calculateSize(df: DataFrame, size: Size): Double = {
    val sizeInBytes = calcRddSize(df.rdd.map(_.toString()))
    size match {
      case s => s.calculate(sizeInBytes)
      case _ => sizeInBytes
    }
  }

}
