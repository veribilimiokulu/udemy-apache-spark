package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Join {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
        .filter(!_.contains("InvoiceNo"))

    println("retailRDD")
   retailRDD.take(5).foreach(println)

    // Quantity ile Unit price çarparak işlem tutarını bulmak
    case class TotalPrice(InvoiceNo:String, Total:Double)
    val retailMapPriceRDD = retailRDD.map(line => {
      val invoiceNo = line.split(";")(0)
      val total = line.split(";")(3).toInt * line.split(";")(5).replace(",",".").toDouble
      TotalPrice(invoiceNo,total)
    })


    println("map splitted satır sayısı: " + retailMapPriceRDD.count())
    retailMapPriceRDD.take(10).foreach(println)

    println("\n retailRDD ve  retailMapPriceRDD satır sayıları: ")
    println(retailRDD.count + " - " + retailMapPriceRDD.count)

  }
}
