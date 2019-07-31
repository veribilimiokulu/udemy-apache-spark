package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountRDD {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    val sc = spark.sparkContext

    val hikayeRDD = sc.textFile("D:/Datasets/omer_seyfettin_forsa_hikaye.txt")
    println(hikayeRDD.count())

    val kelimeler = hikayeRDD.flatMap(satir => satir.split(" "))

    val kelimeSayilari = kelimeler.map(kelime => (kelime,1)).reduceByKey((x,y) => x+y )

    println(kelimeSayilari.count())
    // 837

    kelimeSayilari.take(10).foreach(println)

    val kelimeSayilari2 = kelimeSayilari.map(x => (x._2, x._1))

    kelimeSayilari2.take(15).foreach(println)

    println("**************   EN ÇOK TEKRARLANAN KELİMELER   ***************************")
    kelimeSayilari2.sortByKey(false).take(20).foreach(println)



  }
}
