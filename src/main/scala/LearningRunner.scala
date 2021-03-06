import com.arun.utils.CustomOptimizer.AdditionOptimizerRule
import com.arun.utils.{GigaByte, InitSpark, KiloByte, MegaByte}
import com.arun.utils.EnrichDataFrame._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object LearningRunner extends InitSpark with App {

  val df = loadFileFromResource("covid_19_data.csv")
  df.show()
  println(df.getNumPartition)
  println(df.debugString())
  println(df.calculateSize(MegaByte) + MegaByte.toString)
  df.columns.foreach(println)
  val df1 = df.repartition(col("Country/Region"))
  println(df1.getNumPartition)
  println(df1.repartition(2).getNumPartition)
  println(df1.repartition(2).debugString())
  df1.repartition(2).getPartitionForRow.sample(0.5).show
  additionOptimizationTester(df)


  def additionOptimizationTester(df: DataFrame): Unit = {
    val dfmod = df.withColumn("total", col("Confirmed") + 0.0)
    println(dfmod.queryExecution.optimizedPlan.numberedTreeString)
    //00 Project [SNo#10, ObservationDate#11, Province/State#12, Country/Region#13, Last Update#14, Confirmed#15, Deaths#16, Recovered#17, (Confirmed#15 + 0.0) AS total#59]
    //01 +- Relation[SNo#10,ObservationDate#11,Province/State#12,Country/Region#13,Last Update#14,Confirmed#15,Deaths#16,Recovered#17] csv

    println("After Optimization")

    addOptimizationPlanInSparkSession(spark, Seq(AdditionOptimizerRule))

    val dfWithOptimization = df.withColumn("total", col("Confirmed") + 0.0)
    println(dfWithOptimization.queryExecution.optimizedPlan.numberedTreeString)
    //add optimization implemented
    //00 Project [SNo#10, ObservationDate#11, Province/State#12, Country/Region#13, Last Update#14, Confirmed#15, Deaths#16, Recovered#17, Confirmed#15 AS total#105]
    //01 +- Relation[SNo#10,ObservationDate#11,Province/State#12,Country/Region#13,Last Update#14,Confirmed#15,Deaths#16,Recovered#17] csv

  }


}
