package SparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDOps {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","PairRDD-Ops")

    val insanlarRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\simple_data.csv")
        .filter(!_.contains("sirano"))

    insanlarRDD.take(5).foreach(println)

    def meslekMaasPair(line:String) ={
      val meslek = line.split(",")(3)
      val maas = line.split(",")(5).toDouble

      (meslek,maas)
    }

    println("\nmeslekMaasPairRDD: ")
    val meslekMaasPairRDD = insanlarRDD.map(meslekMaasPair)
    meslekMaasPairRDD.take(5).foreach(println)

    println("\nmeslegeGoreMaas: ")
    val meslegeGoreMaas = meslekMaasPairRDD.mapValues(x => (x,1))
    meslegeGoreMaas.take(16).foreach(println)

    println("\nmeslekMaasRBK: ")
    val meslekMaasRBK = meslegeGoreMaas.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
    meslekMaasRBK.take(16).foreach(println)

    println("\nmeslekOrtalamaMaas: ")
    val meslekOrtalamaMaas = meslekMaasRBK.mapValues(x => x._1 / x._2)
    meslekOrtalamaMaas.take(16).foreach(println)


  }
}
