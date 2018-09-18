package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
      val insanlarRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\simple_data.csv")
      .filter(!_.contains("sirano")) // Başlık satırını atla

    //insanlarRDD.take(10).foreach(println)

    println("\nmap(): \n")
    insanlarRDD.map(x => x.split(",")).take(2).foreach(println)

    println("\nflatMap(): \n")
    insanlarRDD.flatMap(x => x.split(",")).take(10).foreach(println)

  }
}
