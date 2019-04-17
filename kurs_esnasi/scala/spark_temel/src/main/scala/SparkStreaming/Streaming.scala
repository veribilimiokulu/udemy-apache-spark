package SparkStreaming
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Streaming {
  def main(args: Array[String]): Unit = {
    ///////////////////// LOG SEVİYESİNİ AYARLAMA /////////////////////
    Logger.getLogger("org").setLevel(Level.ERROR)


    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    import spark.implicits._
    spark.conf.set("spark.sql.shuffle.partitions","2")

    // Create DataFrame representing the stream of input lines from connection to localhost:9999
    val lines = spark.readStream
      .format("socket")
      .option("host", "gw02.itversity.com")
      .option("port", 9999)
      .option("includeTimeStamp","true")
      .load()

    lines.foreach(println(_))
/*
    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    wordCounts.foreach(println(_))*/
  }
}
