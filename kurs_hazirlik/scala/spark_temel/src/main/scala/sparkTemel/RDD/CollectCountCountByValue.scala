package sparkTemel.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object CollectCountCountByValue {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[4]","CollectCountCountByValue")

    val rakamlar = sc.makeRDD(List(1,2,1,4,2,3,4,1,5,3,4,2,3,4,4))

    println("\ncount(): " + rakamlar.count())

    println("\n collect(): ")
    rakamlar.collect().foreach(println)

    println("\ncountByValue(): ")
    rakamlar.countByValue().foreach(println)
  }
}
