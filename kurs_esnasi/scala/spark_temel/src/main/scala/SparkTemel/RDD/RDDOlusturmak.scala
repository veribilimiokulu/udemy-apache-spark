package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object RDDOlusturmak {
  def main(args: Array[String]): Unit = {
Logger.getLogger("org").setLevel(Level.ERROR)
/*
val spark = SparkSession.builder()
  .master("local[4]")
  .appName("RDD-Olusturma")
  .config("spark.driver.memory","2g")
  .config("spark.executor.memory","4g")
  .getOrCreate()

    val sc = spark.sparkContext
*/

    val conf = new SparkConf()
        .setMaster("local[4]")
        .setAppName("RDD-Olusturma")
        .setExecutorEnv("spark.executor.memory","4g")

    val sc = new SparkContext(conf)


    println("\nList'den RDD oluşturma: ")
    val rddFromList = sc.makeRDD(List(1,2,3,4,5,6,7,8)) // org.apache.spark.rdd.RDD[Int] = ParallelCollectionRDD[0]
    rddFromList.take(3).foreach(println)

    println("\nrddFromListTuple RDD oluşturma: ")
    val rddFromListTuple = sc.makeRDD(List((1,2,3),(4,5),(6,7,8))) //  org.apache.spark.rdd.RDD[Product with Serializable]
    rddFromListTuple.take(3).foreach(println)


    println("\nsparkContext range metoduyla RDD yaratmak")
    // 10.000 ile 20.000 arasında 100'er artan sayılardan RDD oluştur
    val rddByParallelizeCollection4 = sc.range(10000L, 20000L,100) //  org.apache.spark.rdd.RDD[Long]  MapPartitionsRDD[4]
    rddByParallelizeCollection4.take(3).foreach(println)


    println("\nMetin dosyasından  RDD oluşturma: ")
    val rddFromTextFile = sc.textFile("D:/Datasets/OnlineRetail.csv")
rddFromTextFile.take(3).foreach(println)
  }
}
