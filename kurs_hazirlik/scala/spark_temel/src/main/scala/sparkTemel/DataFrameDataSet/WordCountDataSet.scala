package sparkTemel.DataFrameDataSet

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountDataSet {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
    .master("local[4]")
    .appName("Wordcount")
    .config("spark.executor.memory","4g")
    .config("spark.driver.memory","2g")
    .getOrCreate()

    import spark.implicits._

    val hikayeDS = spark.read.textFile("C:/Users/toshiba/SkyDrive/veribilimi.co/udemy-apache-spark/data/omer_seyfettin_forsa_hikaye.txt")
    println(hikayeDS.count())

    hikayeDS.show(1,false)

    // Her bir kelimeyi boşluklarla ayıralım ve başka bir rdd'de tutalım
      val kelimeler = hikayeDS.flatMap(satir => satir.split(" "))

    //println(kelimeler.count())

    //kelimeler.show(truncate=false)

    // En çok tekrarlanan 20 kelime
    // 1. Yöntem
    //kelimeler.groupBy("value").count().orderBy($"count".desc).show()

    //2. Yöntem
    import org.apache.spark.sql.functions.count
    kelimeler.groupBy('value)                                 // gruplanacak sütun
            .agg(count($"value").as("kelimeTekrarSayisi"))    // gruplama fonksiyonu
            .orderBy($"kelimeTekrarSayisi".desc).show()              // sonuçları büyükten küçükten büyüğe sırala
                                                                      // ve sütun ismi değiştir.

  }
}
