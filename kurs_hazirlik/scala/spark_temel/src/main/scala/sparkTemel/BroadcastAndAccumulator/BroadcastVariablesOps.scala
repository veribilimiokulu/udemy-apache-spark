package sparkTemel.BroadcastAndAccumulator

import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkContext, SparkConf}
object BroadcastVariablesOps {
  def main(args: Array[String]): Unit = {

    // LOG SEVİYESİNİ AYARLAMA
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SPARKCONTEXT OLUŞTURMA
    val conf = new SparkConf().setAppName("BroadcastVariablesOps").setMaster("local[4]")
    val sc = new SparkContext(conf)



  }
}
