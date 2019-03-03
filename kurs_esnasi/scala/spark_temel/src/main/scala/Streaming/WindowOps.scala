package Streaming
import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
object WindowOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkContext

    val sc = new SparkContext("local[4]","WindowOps")


    //StreamingContext
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("D:\\checkpoint")

    // DStream from file
    val lines = ssc.textFileStream("D:\\spark-streaming-test")


    //////////////////////// OPERASYON ////////////////////////

val words = lines.flatMap(_.split(" "))

    val mappedWords = words.map(x => (x,1))



    // 1 window()

    // val window = mappedWords.window(Seconds(30), Seconds(10))


    // 2. countByWindow()

    //val countByWindow = mappedWords.countByWindow(Seconds(30), Seconds(10))


    //3 . reduceByKeyAndWindow

      val reduceByKeyAndWindow = mappedWords
        .reduceByKeyAndWindow((x:Int,y:Int) => (x+y),Seconds(60), Seconds(10))
          .map(x => (x._2, x._1))
          .transform(_.sortByKey(false))

    ////////////////////// OPERASYON BİTİŞ ///////////////////
    // Yazdırma
    reduceByKeyAndWindow.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
