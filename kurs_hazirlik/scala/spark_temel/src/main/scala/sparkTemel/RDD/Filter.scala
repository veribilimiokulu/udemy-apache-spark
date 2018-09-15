package sparkTemel.RDD

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
    println("Başlıksız satır sayısı: "+ retailRDDWithHeader.count())


    println(" \n \n")

    val adultSplittedRDD = retailRDDWithHeader.map(line => line.split(";"))
    println(adultSplittedRDD.take(5))

    println(" \n \n")
    println("**********  Birim fiyatı 30'dan küçük olanları filtrele   **************")
    adultRDD.filter(line => line.split(",")(0).toInt < 30).take(5).foreach(println)

    println(" \n \n")
    println("********** Yaşı 30'dan küçük, fnlwgt 10000'den büyük US uyrukluları filtrele   **************")
    adultRDD.filter(line =>
        line.split(",")(0).toInt < 30 && //Yaşı 30'dan küçükler
          line.split(",")(2).trim().toInt > 10000 &&  // fnlwgt 10000'den büyükler
          line.split(",")(13).trim.equals("United-States")) // Uyruk US olanlar
      .take(10).foreach(println)  // sadece 10 tanesini getir bakalım


  }
}
