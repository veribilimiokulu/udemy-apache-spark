package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","reduce")
    val rakamlar = sc.makeRDD(List(1,2,1,2,2,3,5,8)) //  org.apache.spark.rdd.RDD[Int]

    val sum = rakamlar.reduce((x,y) => x+y) // sum: Int = 24
    println("\nToplam: " + sum)


    val rakamlarAggregated = rakamlar.aggregate((0,0))(
      (acc,value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))

    println("\nrakamlarAggregated: " + rakamlarAggregated)

    val ortalama = rakamlarAggregated._1 / rakamlarAggregated._2.toDouble
    println("\nortalama: " + ortalama)
  }
}
