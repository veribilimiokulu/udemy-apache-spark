package sparkTemel.RDD
/*
* fiter transformation için orta dereceli örnekler
* İçinde COFFEE gçen ürünler ve fiyatı 30'dan büyük olan ürünler gibi
* */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object Filter {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("RDD filter() transformation örneği")

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    println("/************************** RDD Transformations filter() ************************************/")


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


    /************** FİLTRELEME ÖRNEKLERİ  ************************************/
    println(" \n \n")
    println("**********  Birim fiyatı 30'dan büyük olanları filtrele   **************")
    retailRDD.filter(line => line.split(";")(3).toInt > 30).take(5).foreach(println)


    println(" \n \n")
    println("********** Ürün tanımı içinde COFFEE geçenleri ve birim fiyatı 20.0'den büyükleri filtrele   **************")
    retailRDD.filter(line =>
        line.split(";")(2).contains("COFFEE") && // COFFEE geçen ürünler (Description)
        line.split(";")(5).trim() // Birim fiyat (UnitPrice)
          .replace(",",".") // Floata çevirmeden önce , ile . yer değiştir.
          .toFloat > 20.0 ) // Float a çevir ve birim fiyatı 20'den büyükleri filtrele
      .take(10).foreach(println)  // sadece 10 tanesini getir bakalım


    println(" \n \n")
    println("********** Fonksiyon ile Ürün tanımı içinde COFFEE geçenleri ve birim fiyatı 20.0'den büyükleri filtrele   **************")

    def coffeePrice20(line:String):Boolean={

      var coffee = line.split(";")(2)
      var unitPrice = line.split(";")(5).trim.replace(",",".").toFloat

      return coffee.contains("COFFEE") && unitPrice > 20.0
    }

    retailRDD.filter(x=>coffeePrice20(x)).take(5).foreach(println)

  }
}
