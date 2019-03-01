package Streaming

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
object QuickExample {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // sparkcon
    val conf = new SparkConf().setMaster("local[4]").setAppName("StreamingQuickExample")

    // StreamingContext
    val ssc = new StreamingContext(conf, Seconds(10))


    val sc = ssc.sparkContext

    // DStream yaratma
    val lines = ssc.textFileStream("D:\\spark-streaming-test")

  val words = lines.flatMap(x => x.split(" "))

    val mappedWord = words.map(x => (x, 1))

    // reduce words
    val wordCounts = mappedWord.reduceByKey((x,y) => x + y) // reduceByKey(_+_)

    val sortedWords = wordCounts.map(x => (x._2, x._1))
        .transform(_.sortByKey(false))
    
    sortedWords.print(50)

    ssc.start()

    ssc.awaitTermination()
  }
}
