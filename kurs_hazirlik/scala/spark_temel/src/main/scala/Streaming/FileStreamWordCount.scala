package Streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.log4j.{Logger, Level}
object FileStreamWordCount {
  def main(args: Array[String]): Unit = {
Logger.getLogger("org").setLevel(Level.ERROR)
/*
      pom.xml dosyasına eklemeyi unutmayın
      <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-streaming_2.11</artifactId>
            <version>2.3.1</version>
        </dependency>
 */
    // Create SparkConf
    val conf = new SparkConf().setMaster("local[2]").setAppName("FileWordCount")

    // Create SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(10))


    val sc = ssc.sparkContext
    // Create DStreams from file source
    val lines = ssc.textFileStream("D:\\spark-streaming-test")

    // Split lines with space and get words
    val words = lines.flatMap(x => x.split(" "))

    // map each word wit 1 and make a pair RDD
    val mappedWords = words.map(x => (x,1))

    // reduce words pair rdd and get te wordcounts
    val wordCounts = mappedWords.reduceByKey((x,y) => (x+y)) //reduceByKey(_+_) can be used

    // Sort word by walue to get most frequent words
    val sortedWordCounts = wordCounts.map(x => (x._2, x._1)).transform(_.sortByKey(false))
    // Write results

    sortedWordCounts.print(20)


    // Start streaming
    // Don't forget start date must be earlier than file's modified date.
    // So create after start or modify files after start
    ssc.start()

    // Wait for stop or error
    ssc.awaitTermination()

  }
}
