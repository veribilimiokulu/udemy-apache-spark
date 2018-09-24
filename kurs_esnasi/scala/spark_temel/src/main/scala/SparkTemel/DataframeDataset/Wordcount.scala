package SparkTemel.DataframeDataset
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
object Wordcount {
  def main(args: Array[String]): Unit = {


  Logger.getLogger("org").setLevel(Level.ERROR)

  // Spark Session oluşturma
  val spark = SparkSession.builder
    .master("local[4]")
    .appName("Wordcount")
    .config("spark.executor.memory","4g")
    .config("spark.driver.memory","2g")
    .getOrCreate()

  import spark.implicits._

  val hikayeDS = spark.read.textFile("C:/Users/toshiba/SkyDrive/veribilimi.co/udemy-apache-spark/data/omer_seyfettin_forsa_hikaye.txt")
  val hikayeDF = hikayeDS.toDF("value")
  //hikayeDF.show(10,truncate = false)

  val kelimeler = hikayeDS.flatMap(x => x.split(" "))

    println(kelimeler.count())

    import org.apache.spark.sql.functions.{count}
    kelimeler.groupBy("value")
      .agg(count('value).as("kelimeSayisi"))
      .orderBy($"kelimeSayisi".desc)
      .show(10)


  }
}
