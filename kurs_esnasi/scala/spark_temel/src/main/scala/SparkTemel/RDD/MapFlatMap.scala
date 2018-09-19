package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapFlatMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("map ve flatMap").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // RDD okuma
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\simple_data.csv")
      .filter(!_.contains("sirano"))
  println("\nOrijinal RDD: ")
    retailRDD.take(3).foreach(println)
    println("\nmap() RDD: ")
    retailRDD.map(x => x.toUpperCase).take(5).foreach(println)

    println("\nflatMap() RDD: ")
    retailRDD.flatMap(x => x.split(",")).map(x => x.toUpperCase).take(5).foreach(println)
  }
}
