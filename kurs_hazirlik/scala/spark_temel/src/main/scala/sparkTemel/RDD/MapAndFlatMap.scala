package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapAndFlatMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
        .filter(!_.contains("InvoiceNo")) // Başlık satırını atla


    println("\n \n ******************* MAP *****************************************")
    // Quantity ile Unit price çarparak işlem tutarını bulmak ve InvoiceNo'dan C harflerini bularak yeni bir sütunda
    // işlemin iptal olup olmadığını boolean olarak yazmak
    val retailMapPriceRDD = retailRDD.map(line => {
      val invoiceNo = line.split(";")(0).takeRight(6).toInt // iptaller için c olduğundan onları atlıyoruz
      val isCancelled = if(line.split(";")(0).startsWith("C")) true else false
      val total = line.split(";")(3).toInt * line.split(";")(5).replace(",",".").toDouble

      invoiceNo+";"+total+";"+isCancelled
    })
    println("retailMapPriceRDD:")
    retailMapPriceRDD.take(10).foreach(println)

    println("\nİptal olanları filtreleme")
    retailMapPriceRDD.filter(x=> x.split(";")(2) == "true").take(10).foreach(println)

    println("\nİptal olanların sayısı: \n" + retailMapPriceRDD.filter(x=> x.split(";")(2) == "true").count())


    println("map splitted satır sayısı: " + retailMapPriceRDD.count())





    println("\n \n ******************* FLATMAP *****************************************")
    val retailFlatMapSplittedRDD = retailRDD.flatMap(x => x.split(";"))
    println("flatMap splitted satır sayısı: " + retailFlatMapSplittedRDD.count())
    retailFlatMapSplittedRDD.take(10).foreach(println)


    println()
    val retailFlatMapToUpperRDD = retailRDD.flatMap(x => x.split(";")).map(x=>x.toUpperCase)
    println("retailFlatMapToUpperRDD")
    retailFlatMapToUpperRDD.take(10).foreach(println)





  }
}
