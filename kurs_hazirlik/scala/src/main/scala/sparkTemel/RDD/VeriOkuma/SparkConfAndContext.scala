package sparkTemel.RDD.VeriOkuma
import org.apache.spark.{SparkConf, SparkContext}


object SparkConfAndContext {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("SparkConfAndContext Örnek")
      .setExecutorEnv("spark.driver.memory","1g")
      .setExecutorEnv("spark.executor.memory","4g")

    val sc = new SparkContext(conf)

    val myRDD = sc.parallelize(Seq(1,2,3,4,5,6,7,8))
    myRDD.take(8).foreach(println)

    sc.getConf.getAll.foreach(println)
  }
}
