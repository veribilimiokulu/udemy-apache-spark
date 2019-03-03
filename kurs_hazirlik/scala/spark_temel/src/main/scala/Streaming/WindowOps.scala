package Streaming
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Logger, Level}

object WindowOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkContext
    val sc = new SparkContext("local[2]","WindowOps")

    // SparkStreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // Checkpoint
    ssc.checkpoint("D:\\checkpoint")

    // DStreams from file source
    val lines = ssc.textFileStream("D:\\spark-streaming-test")

///////////////// OPERASYON ////////////////////////////////////////////////

    val words = lines.flatMap(_.split(" "))

    // map words
    val mappedWords = words.map(x => (x,1))


    // 1. window()
   // val window = mappedWords.window(Seconds(30), Seconds(10))

    // 2. CountByWindow()
    val countByWindow = mappedWords.countByWindow(Seconds(30), Seconds(10))

    // 3.
   val windowedAndReducedCounts = mappedWords
     .reduceByKeyAndWindow((x:Int, y:Int) => (x+y),Seconds(30), Seconds(10))
     .map(x => (x._2, x._1))
     .transform(_.sortByKey(false))

    ///////////////// OPERASYON BİTİŞ ////////////////////////////////////////////////
    // Sonuçları ekrana yazdır
    windowedAndReducedCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
