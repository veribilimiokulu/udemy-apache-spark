package Streaming.StructuredStreaming
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
object ReadFromConsole {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("ReadFromConsole")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    // Kaynaktan okuma
    val lines = spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load()


    // Operasyon
    val words = lines.as[String].flatMap(_.split(" "))

    val wordcount = words.groupBy("value").count()
      .sort(desc("count"))



    // Sonuç Çıktı
    val query = wordcount.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
