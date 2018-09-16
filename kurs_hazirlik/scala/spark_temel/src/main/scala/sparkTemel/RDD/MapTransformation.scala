package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapTransformation {
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
    println("map splitted satır sayısı: " + retailMapPriceRDD.count())
    println("retailMapPriceRDD:")
    retailMapPriceRDD.take(10).foreach(println)

    println("\nİptal olanları filtreleme")
    retailMapPriceRDD.filter(x=> x.split(";")(2) == "true").take(10).foreach(println)

    println("\nİptal olanların sayısı: \n" + retailMapPriceRDD.filter(x=> x.split(";")(2) == "true").count())




    println("\n \n ************* MAP İLE PAIR RDD YARATMAK ve İPTAL OLAN SATIŞLARIN TOPLAM TUTARINI HESAPLAMAK *********")
  // Aşağıdaki bölümde iptal edilen satışların toplam tutarını bulacağız

    // case class ile ihtiyacımız olan özellikleri yapısal hale getirelim
    case class TotalCancelledPrice(isCancelled:Boolean, total:Double)

    // map dönüşümü ile içinde case class(TotalCancelledPrice) olan bir rdd yaratalım
    val retailTotalCancelledPriceRDD = retailRDD.map(line => {
      //val invoiceNo = line.split(";")(0).takeRight(6).toInt // iptaller için c olduğundan onları atlıyoruz
      val isCancelled = if(line.split(";")(0).startsWith("C")) true else false
      val total = line.split(";")(3).toInt * line.split(";")(5).replace(",",".").toDouble

      TotalCancelledPrice(isCancelled,total)
    })
    // Oluşan yeni rdd türü aşağıdadır
    // TotalCancelledPrice: org.apache.spark.rdd.RDD[TotalPrice] = MapPartitionsRDD[3] at map at <console>:16
    // Yeni rdd'yi görelim
    println("\nİçinde case class taşıyan yeni TotalCancelledPrice RDD")
    retailTotalCancelledPriceRDD.take(10).foreach(println)


    println("\nİptal edilen toplam tutar: ")
    retailTotalCancelledPriceRDD
      .map(x => (x.isCancelled, x.total))  // anahtar değer oluştur. Çünkü reduceByKey bunu istiyor
      .reduceByKey((x,y) => x+y) // true ve false olan anahtarlara göre değerleri topla
      .filter(x => x._1 == true)  // true değer iptal edilenlerdi onları filtrele diğerleri gelmesin
      .map(x => x._2) // Benim ihtiyacım sadece rakam. Boşuna true değerini yazma
      .take(2) // Sonuçtan ikisini al. Bir tane bekliyoruz ama yukarıdaki filtreyi kaldırınca true ve false gelir
      .foreach(println) // Sonucu yazdır.




  }
}
