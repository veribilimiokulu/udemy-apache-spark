package sparkTemel.RDD

import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext

object groupBy {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","groupBy")

    println("\nrakamlarRDD")
    val rakamlarRDD = sc.makeRDD(List(1,2,1,4,2,3,4,1,5,3,4,2,3,4,4)) // org.apache.spark.rdd.RDD[Int]
    rakamlarRDD.take(10).foreach(println)

    println("\nrakamlarGroupBy")
    val rakamlarGroupBy = rakamlarRDD.groupBy(x => x) //  org.apache.spark.rdd.RDD[(Int, Iterable[Int])]
    rakamlarGroupBy.foreach(println)


  }
}
