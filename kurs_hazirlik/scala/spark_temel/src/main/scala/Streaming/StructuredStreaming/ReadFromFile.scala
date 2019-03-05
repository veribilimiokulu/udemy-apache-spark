package Streaming.StructuredStreaming
/*
Erkan ŞİRİN
Spark Structured Streaming ve file stream kullanılarak wordcount uygulaması
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
object ReadFromFile {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromFile")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._

   // Satırları oku
    val lines = spark.readStream
        .format("text")
        .load("D:\\spark-streaming-test")
/*
Ham hali
+--------------------+
|               value|
+--------------------+
|Ömer Seyfettin - ...|
|Akdeniz’in, kahra...|
|Askerler onun yak...|
|– Kaç yıldır tuts...|
|             – Kırk!|
|       – Nerelisin ?|
|        – Edremitli.|
|          – Adın ne?|
+--------------------+

 */

    // Kelimeleri ayır
    val words = lines.as[String].flatMap(_.split(" "))

    // Wordconts
    val wordCounts = words.groupBy("value").count()
      .sort(desc("count"))

    // Streaming başlasın
    val query = wordCounts.writeStream
      .outputMode("complete") //sorguda aggregation varsa complete
      .format("console")
      .start()

    query.awaitTermination()
    /*
    Beklenen sonuç:
    -------------------------------------------
Batch: 1
-------------------------------------------
+--------+-----+
|   value|count|
+--------+-----+
|     bir|   33|
|       –|   31|
|        |   27|
|  Forsa-|   24|
|     yıl|    8|
|    diye|    6|
|    Kırk|    5|
|   dedi.|    5|
|    Türk|    5|
|    onun|    5|
|   doğru|    5|
|   büyük|    4|
|   Yirmi|    4|
|     Ben|    4|
|     Ama|    4|
|  tutsak|    4|
|    gibi|    4|
|gibiydi.|    3|
|başladı.|    3|
|   kadar|    3|
+--------+-----+
only showing top 20 rows
     */
  }
}
