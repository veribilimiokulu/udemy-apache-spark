package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.log4j.{Logger, Level}


object WriteToKafka {
  def main(args: Array[String]): Unit = {
    /*
    pom.xml dosyasında aşağıdaki dependency'nin varlığını kontrol etmeyi unutmayın.
     <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
            <version>2.3.1</version>
        </dependency>
     */

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


    // Kafka'ya yazmak için hazırlık

    val df2 = df.withColumn("key", F.col("ID")).drop("ID")


    val df3 = df2.withColumn( "value",
        F.concat( F.col("TV"), F.lit(","),
        F.col("Radio"), F.lit(","),
        F.col("Newspaper"), F.lit(","),
        F.col("Sales")
    )).drop("TV","Radio","Newspaper","Sales")

    df3.show()


    df3.write
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("topic","deneme")
      .save()








  }
}
