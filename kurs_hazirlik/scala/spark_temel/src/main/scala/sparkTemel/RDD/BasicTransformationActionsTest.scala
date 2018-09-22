package sparkTemel.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger,Level}

object BasicTransformationActionsTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
// SORU-1
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("Test")
      .set("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","3g")

    val sc = new SparkContext(sparkConf)

    println("Merhaba Spark")

    //***************************************************************

    // SORU-2
    println("\nSORU-2")
    val rdd = sc.parallelize(List(3,7,13,15,22,36,7,11,3,25))
    rdd.collect().foreach(println(_))

    //****************************************************************

    // SORU-3
    println("\nSORU-3")
    val cumle = sc.makeRDD(List("Spark'ı öğrenmek çok heyecan verici."))
    cumle.map(x => x.toUpperCase).collect().foreach(println(_))

    //******************************************************************


    // SORU-4
    // Adresteki metin dosyasını bilgisayara kaydettikten sonra
    val metin = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\docs\\Ubuntu_Spark_Kurulumu.txt")
    println("\nSORU-4 Metindeki toplam satır sayısı: " + metin.count())

  //**********************************************************************

    //SORU-5
    val kelimeSayisi = metin.flatMap(x => x.split(" ")).count()
    println("\nSORU-5 Kelime sayısı: " + kelimeSayisi)

    //**********************************************************************


    // SORU-6
    println("\nSORU-6 Kesişim kümesi (ortak rakamlar): ")
    val rdd2 = sc.makeRDD(List(1,2,3,4,5,6,7,8,9,10))
    rdd.intersection(rdd2).collect.foreach(println(_))

    //**********************************************************************


    // SORU-7
    println("\nSORU-7 Tekil rakamlardan oluşan RDD: ")
    val liste = List(3,7,13,15,22,36,7,11,3,25)
    val tekilListeRDD = sc.parallelize(liste.distinct)
    tekilListeRDD.collect().foreach(println(_))

    //**********************************************************************


    // SORU-8
    println("\nSORU-8 Her bir rakamın frekansı: ")
    val frekansRDD = rdd.map(x => (x,1))
                        .reduceByKey((x,y) => x + y)
                        .collect()
                        .foreach(println(_))

  }
}
