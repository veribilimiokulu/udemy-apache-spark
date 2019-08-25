package SparkTemel.DataframeDataset

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.log4j.{Logger, Level}


object WriteToKafka {
  def main(args: Array[String]): Unit = {

    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("WriteToKafka")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    val df = spark.read.format("csv")
      .option("header", true)
      .load("D:/Datasets/Advertising.csv")

    df.show(2)

    val df2 = df.withColumn("key", F.col("ID")).drop("ID")
      .withColumn("value", F.concat(
        F.col("TV"), F.lit(","),
        F.col("Radio"), F.lit(","),
        F.col("Newspaper"), F.lit(","),
        F.col("Sales")
      )).drop("TV","Radio","Newspaper","Sales")

    df2.show(2)

    df2.write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","deneme")
      .save()







  }
}
