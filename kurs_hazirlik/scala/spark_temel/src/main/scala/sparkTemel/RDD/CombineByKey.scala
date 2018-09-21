package sparkTemel.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object CombineByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","CombineByKey")

    val myTuple = List((1,2),(3,4),(3,6))
    val rdd = sc.parallelize(myTuple)

    println("\nCombineByKey sonucu: ")
    val result = rdd.combineByKey((v) => (v,1),
      (acc: (Int,Int), v) => (acc._1 + v, acc._2 + 1),
      (acc1:(Int,Int), acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    result.take(3).foreach(println(_))


    println("\nSonuçtan ortalamaları bulma")
    val mappedResult = result.map{ case (key, value) => (key, value._1 / value._2.toFloat)}

    mappedResult.take(2).foreach(println(_))
  }
}
