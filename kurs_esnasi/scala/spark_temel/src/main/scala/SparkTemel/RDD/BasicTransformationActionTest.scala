package SparkTemel.RDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Logger, Level}
object BasicTransformationActionTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SORU-1
    val sparkConf = new SparkConf()
      .setAppName("Test")
      .setMaster("local[2]")
      .set("spark.driver.memory","2g")
      .setExecutorEnv("spark.executor.memory","3g")

    val sc = new SparkContext(sparkConf)
    println("Merhaba Spark")


    // SORU-2
    val rddRakam = sc.parallelize(List(3,7,13,15,22,36,7,11,3,25))
    println("\nSORU-2:")
    rddRakam.foreach(println(_))

    // SORU-3
    println("\nSORU-3: ")
    val metinRDD = sc.makeRDD(List("Spark'ı öğrenmek çok heyecan verici"))
    metinRDD.map(x => x.toUpperCase).foreach(println(_))



    // SORU-4
    println("\nSORU-4: ")
    val ubuntu = sc.textFile("C:\\Users\\toshiba\\Desktop\\ubuntu.txt")
    println(ubuntu.count())


    //SORU-5
    println("\nSORU-5: ")

    val kelimeSayisi = ubuntu.flatMap(x => x.split(" ")).count()
    println(kelimeSayisi)

    //SORU-6
    println("\nSORU-6: ")
    val rddRakam2 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10))
    rddRakam.intersection(rddRakam2).collect().foreach(println(_))

    //SORU-7
    println("\nSORU-7: ")

    val rakamTekil = List(3,7,13,15,22,36,7,11,3,25).distinct
    val rakamTekilRDD = sc.parallelize(rakamTekil)
    rakamTekilRDD.collect().foreach(println(_))

    //SORU-8
    println("\nSORU-8: ")

    val rakam = List(3,7,13,15,22,36,7,11,3,25)
    val rakamRDD = sc.parallelize(rakam)
    rakamRDD.map(x => (x,1))
      .reduceByKey((x,y) => x + y)
      .collect().foreach(println(_))


    sc.getConf.getAll.foreach(println(_))
  }
}
