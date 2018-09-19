package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
      val insanlarRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\simple_data.csv")
      .filter(!_.contains("sirano")) // Başlık satırını atla

    //insanlarRDD.take(10).foreach(println)

    println("\nmap(): \n")
    insanlarRDD.map(x => x.toUpperCase).take(2).foreach(println)

    println("\nflatMap(): \n")
    insanlarRDD.flatMap(x => x.toUpperCase).take(10).foreach(println)


    println("\nwordcount sonuçlarını iyileştirme")
    val hikayeRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\omer_seyfettin_forsa_hikaye.txt")

    println("\nkelimeler: ")
    val  stopWords = ", ; . ? ! ve ile ama ki ne kim nasıl hangi"
    val kelimeler = hikayeRDD.flatMap(_.split(" ")).filter(x => x.contains(stopWords))
    kelimeler.take(10).foreach(println)

    val kelimeSayilari = kelimeler.countByValue()
    println("\nkelimeSayıları: ")
    kelimeSayilari.take(20).foreach(println)

    kelimeSayilari.map(x => (x._1,x._2.toFloat))
  }
}
