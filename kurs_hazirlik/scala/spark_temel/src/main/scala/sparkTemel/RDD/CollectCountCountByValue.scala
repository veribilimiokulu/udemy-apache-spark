package sparkTemel.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object CollectCountCountByValue {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[4]","CollectCountCountByValue")

    val rakamlar = sc.makeRDD(List(1,2,1,4,2,3,4,1,5,3,4,2,3,4,4))  // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0]

    println("\ncount(): " + rakamlar.count())  // Long = 15

    println("\n collect(): ")
    rakamlar.collect().foreach(println)  //  Array[Int] = Array(1, 2, 1, 4, 2, 3, 4, 1, 5, 3, 4, 2, 3, 4, 4)

    println("\ncountByValue(): ")
    rakamlar.countByValue().foreach(println) //  scala.collection.Map[Int,Long] = Map(5 -> 1, 1 -> 3, 2 -> 3, 3 -> 3, 4 -> 5)

  }
}
