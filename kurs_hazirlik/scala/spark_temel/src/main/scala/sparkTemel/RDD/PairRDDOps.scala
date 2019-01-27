package sparkTemel.RDD
/*
* Mesleklere göre ortalam maaşı bulmak için
* mapValues ve reduceByKey kullanımı
* */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object PairRDDOps {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","ParRDD-Operations")

    val insanlarRDD = sc.textFile("D:\\Udemy_Spark_Kursu\\kodlar\\data\\simple_data.csv")
        .filter(!_.contains("sirano"))

    insanlarRDD.take(4).foreach(println)

    // Mesleklere göre ortalama kazançları bulma
    def meslekMaas(line:String) ={
      val meslek = line.split(",")(3)
      val maas = line.split(",")(5).toDouble

      (meslek, maas)
    }


    val meslekMaasPairRDD = insanlarRDD.map(meslekMaas)


    println("\nmeslekMaasPairRDD map() sonucu: ")
    meslekMaasPairRDD.take(4).foreach(println)


    println("\nmeslegeGoreMaasMap mapValues() sonucu: ")
    val meslegeGoreMaasMap = meslekMaasPairRDD.mapValues(x=>(x,1))
    meslegeGoreMaasMap.take(4).foreach(println)


    println("\nmeslekMaasRBK reduceByKey sonucu: ")
    val meslekMaasRBK = meslegeGoreMaasMap.reduceByKey((x,y) => (x._1 + y._1,  x._2 + y._2))
    meslekMaasRBK.take(13).foreach(println)


    println("\nmeslekOrtMaas: ")
    val meslekOrtMaas = meslekMaasRBK.mapValues(x => x._1 / x._2)
    meslekOrtMaas.take(13).foreach(println)

  }
}
