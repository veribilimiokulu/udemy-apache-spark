package MachineLearning.Classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{SparkSession}
object ClassificationWithIris {
  def main(args: Array[String]): Unit = {

    // Log seviyesi ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)

    //SparkSession
    val spark = SparkSession.builder()
      .appName("ClassificationWithIris")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    //
    val df = spark.read.format("csv")
      .option("header",true)
      .option("sep",",")
      .option("inferSchema",true)
      //.load("https://www.veribilimi.co/data/iris.csv")

    df.show()
  }
}
