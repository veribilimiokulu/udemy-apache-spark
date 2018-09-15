package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object RDDOlusturmak {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)


    //SparkConf ve SparkContext
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("RDD-Olusturmak")
      .setExecutorEnv("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

/*
    // SparkSession, SparkConf kullanmadan
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    //sparkContext oluşturma
    val sc = spark.sparkContext

    // SparkSession SparkConf kullanarak
    val spark = SparkSession.builder
      .config(conf)
      .getOrCreate()
    val sc = spark.sparkContext

*/
    // Collection'ı dağıtarak oluşturma
    val rddByParallelizeCollection = sc.parallelize(Seq(1,2,3,4,5,6,7,8))
    rddByParallelizeCollection.take(8).foreach(println)


    sc.getConf.getAll.foreach(println)
  }
}
