package sparkTemel.RDD

import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","ReduceByKey-Min-Max")

    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    retailRDD.take(5).foreach(println)


    println("\nHer bir ürünün sahip olduğu en düşük/yüksek fiyatı bulmak: ")

    def getStockNoAndPrice(line:String)={
      val stockNo = line.split(";")(1)
      val price = line.split(";")(5).replace(",",".").toFloat

      (stockNo,price)
    }

    val stockAndPriceRDD = retailRDD.map(getStockNoAndPrice)

    stockAndPriceRDD.take(5).foreach(println)

    println("\nEn düşük fiyatlar: ")
    val stockAndPriceRBK = stockAndPriceRDD.reduceByKey((x,y) => (scala.math.min(x,y)))
    stockAndPriceRBK.take(10).foreach(println)


    println("\nEn yüksek fiyatlar: ")
    val stockAndPriceRBK2 = stockAndPriceRDD.reduceByKey((x,y) => (scala.math.max(x,y)))
    stockAndPriceRBK2.take(10).foreach(println)



  }
}
