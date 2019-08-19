package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RDDOlusturmak {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

/*
    //SparkConf ve SparkContext
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD-Olusturmak")
      .setExecutorEnv("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

*/
    // SparkSession, SparkConf kullanmadan
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    //sparkContext oluşturma
    val sc = spark.sparkContext
    /*
        // SparkSession SparkConf kullanarak
        val spark = SparkSession.builder
          .config(conf)
          .getOrCreate()
        val sc = spark.sparkContext

    */
    // SparkConf bilgileri
    println("SparkCof bilgileri: ")
    sc.getConf.getAll.foreach(println)
    println("Executor memory bilgileri: ")
    sc.getExecutorMemoryStatus.foreach(println)


    /************** Collection'ı dağıtarak RDD oluşturma   *************************/
    println("\nScala Seq ile RDD yaratmak")
    val rddByParallelizeCollection = sc.parallelize(Seq(1,2,3,4,5,6,7,8)) // org.apache.spark.rdd.RDD[Int]  ParallelCollectionRDD[0]
    rddByParallelizeCollection.take(3).foreach(println)

    println("\nScala tuple ile RDD yaratmak")
    val rddByParallelizeCollection2 = sc.makeRDD(Seq((1,2),(3,4),(5,6),(7,8))) //  org.apache.spark.rdd.RDD[(Int, Int)]  ParallelCollectionRDD[1]
    rddByParallelizeCollection2.take(3).foreach(println)

    println("\n Scala List'i kullanarak RDD yaratmak")
    val rddByParallelizeCollection3 = sc.parallelize(List(10,20,30,40,50,60,70,80)) // org.apache.spark.rdd.RDD[Int]  ParallelCollectionRDD[2]
    rddByParallelizeCollection3.take(3).foreach(println)

    println("\nsparkContext range metoduyla RDD yaratmak")
    // 10.000 ile 20.000 arasında 100'er artan sayılardan RDD oluştur
    val rddByParallelizeCollection4 = sc.range(10000L, 20000L,100) //  org.apache.spark.rdd.RDD[Long]  MapPartitionsRDD[4]
    rddByParallelizeCollection4.take(3).foreach(println)

    println("\nsql.Row lardan RDD yaratmak")
    val abc = org.apache.spark.sql.Row((1,2),(3,4),(5,6),(7,8)) //  org.apache.spark.sql.Row
    val rddByRows = sc.makeRDD(List(abc)) // org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = ParallelCollectionRDD[5]
    rddByRows.take(3).foreach(println)



    /********************** Metin dosyalarından RDD oluşturmak *****************************/
    println("\nMetin dosyalarından RDD oluşturmak")
    val rddFromTextFile = sc.textFile("D:\\Datasets\\OnlineRetail.csv")
    rddFromTextFile.take(5).foreach(println)

    println("Okunan metin dosyasının satır sayısı: ")
    println(rddFromTextFile.count())

  }
}
