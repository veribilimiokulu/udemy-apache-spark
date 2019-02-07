package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object CountByValue {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("CountByValue").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma ve başlık satırını filtreleme
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo")) // Başlık satırını atla


    println("\ncountByValue ile işlem sayısı en çok olan 10 müşteriyi bulma yöntem-1\n")
    println("\nmap() ile CustomerID'yi seçme ve retailCustomerIDRDD'ye atama: \n")
    val retailCustomerIDRDD = retailRDD.map(_.split(";")(6))
    retailCustomerIDRDD.take(3).foreach(println)

    println("\ncountByValue() ile her bir farklı müşterino kaç defa tekrarlanmış map oluştur (CustomerID, alışveriş_sayısı): \n")
    val retailCustomerIDcountByValue = retailCustomerIDRDD.countByValue()
    retailCustomerIDcountByValue.take(3).foreach(println)


    println("\ntoSeq.sortBy(_._2) ile Map'i Seq'e çevirme ve alışverişsayısına göre sıralama: \n")
    val retailCustomerIDSort = retailCustomerIDcountByValue.toSeq.sortBy(_._2).reverse
    retailCustomerIDSort.take(10).foreach(println)



    println("\ncountByValue ile işlem sayısı en çok olan 10 müşteriyi bulma yöntem-2\n")
    println("\ncountByValue\n")
    retailRDD.map(x => x.split(";")(6)) // satırı ; ile ayır ve CustomerID indexi olan 6'yı seç. Burası bir string
      .countByValue() // her bir farklı müşterino kaç defa tekrarlanmış map oluştur (CustomerID, alışverişsayısı) scala.collection.Map[String,Long] = Map(17079 -> 2, 16997 -> 12, 14779
      .toSeq.sortBy(_._2).reverse // Map'i Seq'e çevir ve alışverişsayısına göre sıralar.  Seq[(String, Long)] = ArrayBuffer((000000,135080), (17841,7983), (14911,5903)
      .take(10)  // Sonuçtan 10 tanesini al
      .foreach(println)


  }
}
