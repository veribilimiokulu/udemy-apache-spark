package Streaming.StructuredStreaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromFile {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    //SparkSession
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromFile")
      .config("spark.driver.memory","2g")
      .config("spark.executormemory","4g")
      .getOrCreate()

    import spark.implicits._

    // Satırları oku
    val lines = spark.readStream
      .format("text")
      .load("D:\\spark-streaming-test")


    // Transformasyon

    // get words
      val words = lines.as[String].flatMap(x=>x.split(" "))

    // aggraget words
    val wordcounts = words.groupBy("value").count()
      .sort(desc("count"))



    // Sonuç



    // Streaming Start
    val query = wordcounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
