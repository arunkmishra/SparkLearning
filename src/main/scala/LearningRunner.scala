import com.arun.utils.{GigaByte, InitSpark, KiloByte, MegaByte}
import com.arun.utils.EnrichDataFrame._


object LearningRunner extends InitSpark with App {

  val df = loadFileFromResource("covid_19_data.csv")
  df.show()
  println(df.getNumPartition)
  println(df.debugString())
  println(df.calculateSize(MegaByte) + MegaByte.toString)
}
