package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val retailRDDWithHeader = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")

    println("RDD ham hali")
    retailRDDWithHeader.take(5).foreach(println)

    //başlıkla beraber satır sayısı
    println("Başlıkla beraber satır sayısı: "+ retailRDDWithHeader.count())

    // Başlıktan kurtulalım
    val retailRDD = retailRDDWithHeader.mapPartitionsWithIndex(
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    )
    //başlıksız satır sayısı
    println("Başlıksız satır sayısı: "+ retailRDD.count())

   // println(retailRDD.first())

    println(" \n \n")
    println("**********  Birim miktarı 30'dan büyük olanları filtrele   **************")

    retailRDD.filter(x => x.split(";")(3).toInt > 30) // Quantity büyük 30
      .take(5)
      .foreach(println)

    println(" \n \n")
    println("********** Ürün tanımı içinde COFFEE geçenleri ve birim fiyatı 20.0'den büyükleri filtrele   **************")

    retailRDD.filter(x => x.split(";")(2).contains("COFFEE") &&
    x.split(";")(5).trim.replace(",",".") // trim ve float için , . yer değiş
      .toFloat > 20.0F) // float a çevir
      .take(5) // sonucun beş tanesini driver a getir
      .foreach(println) // yazdır


    println(" \n \n")
    println("********** Fonksiyon ile Ürün tanımı içinde COFFEE geçenleri ve birim fiyatı 20.0'den büyükleri filtrele   **************")

    def coffePrice20(line:String):Boolean={
      var sonuc = true
      var Description:String = line.split(";")(2)
      var UnitPrice:Float =line.split(";")(5).trim.replace(",",".").toFloat

      sonuc = Description.contains("COFFEE") && UnitPrice > 20.0
      sonuc
    }

    retailRDD.filter(x=> coffePrice20(x))
      .take(5)
      .foreach(println)













  }
}
