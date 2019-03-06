package Streaming.StructuredStreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromCsv {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromCsv")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._

    // Şema
    val mySchema = new StructType()
        .add("sirano", IntegerType)
      .add("isim", StringType)
      .add("yas", IntegerType)
      .add("meslek", StringType)
      .add("sehir", StringType)
      .add("aylik_gelir", DoubleType)


    // Veri Okuma
    val df = spark.readStream
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .schema(mySchema)
      .load("D:\\spark-streaming-test")

// mesleklere göre ortalama gelir
    val meslekOrtGelir = df.groupBy("meslek").agg(avg("aylik_gelir").as("ortGelir"))
      .sort(desc("ortGelir"))


    // Output

    val query = meslekOrtGelir.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()


  }
}
