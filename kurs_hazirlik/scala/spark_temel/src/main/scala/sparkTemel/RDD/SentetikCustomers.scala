package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SentetikCustomers {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","SentetikCustomers")

    val retailRDD = sc.textFile("C:/Users/toshiba/SkyDrive/veribilimi.co/Datasets/OnlineRetail.csv")
      .filter(x => !x.contains("InvoiceNo"))
    retailRDD.top(5).foreach(println)

    def takeCustomerID(line:String)={
      val customerID = line.split(";")(6)

      customerID
    }

    println("\nBenzersiz customerID'ler: ")
    val customerIDsRDD = retailRDD.map(takeCustomerID).distinct()
    customerIDsRDD.take(10).foreach(println)

    var totalDistinctID = customerIDsRDD.count()
    println("\nToplam ID sayısı: " + totalDistinctID)

    println("\nİsimler: ")
    val isimlerRDD = sc.textFile("C:/Users/toshiba/SkyDrive/veribilimi.co/Datasets/isimler_4373.txt")
    isimlerRDD.take(10).foreach(println)


  }
}
