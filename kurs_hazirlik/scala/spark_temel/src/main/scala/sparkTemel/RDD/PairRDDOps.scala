package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","ParRDD-Operations")

    val insanlarRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\data\\simple_data.csv")
        .filter(!_.contains("sirano"))

    insanlarRDD.take(4).foreach(println)

    // Mesleklere göre ortalama kazançları bulma
    def meslekMaas(line:String) ={
      val sehir = line.split(",")(3)
      val maas = line.split(",")(5).toDouble

      (sehir, maas)
    }


    val meslekMaasPairRDD = insanlarRDD.map(meslekMaas)


    println("\nsehirMaasPairRDD: ")
    meslekMaasPairRDD.take(4).foreach(println)


    println("\nsehireGoreMaasMap: ")
    val meslegeGoreMaasMap = meslekMaasPairRDD.mapValues(x=>(x,1))
    meslegeGoreMaasMap.take(4).foreach(println)


    println("\nsehirMaasRBK: ")
    val meslekMaasRBK = meslegeGoreMaasMap.reduceByKey((x,y) => (x._1 + y._1,  x._2 + y._2))
    meslekMaasRBK.take(13).foreach(println)


    println("\nsehirMaasRBK: ")
    val meslekOrtMaas = meslekMaasRBK.mapValues(x => x._1 / x._2)
    meslekOrtMaas.take(13).foreach(println)

  }
}
