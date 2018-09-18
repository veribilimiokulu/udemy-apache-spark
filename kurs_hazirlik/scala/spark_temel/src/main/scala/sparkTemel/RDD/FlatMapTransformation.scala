package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
      val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo")) // Başlık satırını atla


    println("\n \n ******************* FLATMAP *****************************************")
    val retailFlatMapSplittedRDD = retailRDD.flatMap(x => x.split(";"))
    println("flatMap splitted satır sayısı: " + retailFlatMapSplittedRDD.count())
    retailFlatMapSplittedRDD.take(10).foreach(println)


    println("flatMap ile her kelimeyi büyük harf yapma: ")
    val retailFlatMapToUpperRDD = retailRDD.flatMap(x => x.split(";")).map(x=>x.toUpperCase)
    println("retailFlatMapToUpperRDD")
    retailFlatMapToUpperRDD.take(2).foreach(println)


  }
}
