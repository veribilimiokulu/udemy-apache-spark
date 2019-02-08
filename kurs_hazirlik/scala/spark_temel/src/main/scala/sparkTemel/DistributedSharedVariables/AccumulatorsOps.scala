package sparkTemel.DistributedSharedVariables

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.LongAccumulator

object AccumulatorsOps {
  def main(args: Array[String]): Unit = {
    // Log seviyesi ayarlama: sadece hataları göstersin
    Logger.getLogger("org").setLevel(Level.ERROR)

    /////////////////////////////  SPARKCONTEXT OLUŞTURMA /////////////////////////////////////////////////////////
    //==========================================================================================================
    val conf = new SparkConf().setAppName("Join").setMaster("local[4]")
    val sc = new SparkContext(conf)



    /////////////////////////////  VERİ OKUMA SAFHASI /////////////////////////////////////////////////////////
    //==========================================================================================================
    // OnlineRetail.csv okuma
    val onlineRetailRDD = sc.textFile("D:\\Datasets\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo")) // İlk başlık satırından kurtulma
    println("\n onlineRetailRDD ilk göz atma: ")
    onlineRetailRDD.take(5).foreach(println(_))

    // United Kingdom için accumulator oluşturalım

    val accUK = sc.longAccumulator


   val totalUK = onlineRetailRDD.map(line => {
      if(line.split(";")(7).contains("United Kingdom")){
        accUK.add(1L)
      }
      accUK.value
    })

   println("UK sayısı: ")
    totalUK.take(3).foreach(println(_))

  }
}
