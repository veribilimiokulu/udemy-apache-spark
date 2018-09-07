package sparkTemel.RDD.Transformations

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object MapAndFlatMap {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("RDD map() ve flatMap() transformation örneği")

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)


    // RDD okuma
    val adultRDDWithHeader = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\adult_dataset\\adult.data")

    println("RDD ham hali")
    adultRDDWithHeader.take(5).foreach(println)


    //başlıkla beraber satır sayısı
    println("Başlıkla beraber satır sayısı: "+ adultRDDWithHeader.count())


    // Başlıktan kurtulalım
    val adultRDD = adultRDDWithHeader.mapPartitionsWithIndex(
      (idx, iter) => if (idx == 0) iter.drop(1) else iter
    )
    //başlıksız satır sayısı
    println("Başlıksız satır sayısı: "+ adultRDD.count())

    println(" \n \n")
    val adultMapSplittedRDD = adultRDD.map(line => line.split(","))
    println("map splitted satır sayısı: " + adultMapSplittedRDD.count())


    val adultFlatMapSplittedRDD = adultRDD.flatMap(x => x.split(","))
    println("flatMap splitted satır sayısı: " + adultFlatMapSplittedRDD.count())


    val somOfCols = adultRDD.map(line => line.split(",")(1))
  }
}
