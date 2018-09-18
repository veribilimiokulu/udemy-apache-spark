package SparkTemel.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object MapTransformation {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("sparkTemelRDD").setMaster("local[4]")
    val sc = new SparkContext(conf)

    // RDD okuma
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    println(retailRDD.first())
    case class CancelledPrice(isCancelled:Boolean, total:Double)
    val retailTotal = retailRDD.map(x => {
      var isCancelled:Boolean = if(x.split(";")(0).startsWith("C")) true else false
      var quantity:Double = x.split(";")(3).toDouble
      var price:Double = x.split(";")(5).replace(",",".").toDouble

      var total:Double = quantity*price
      CancelledPrice(isCancelled, total)
    })

    retailTotal.take(5).foreach(println)

    println("\nİptal edilen toplam tutar: ")
    retailTotal.map(x => (x.isCancelled, x.total))
      .reduceByKey((x,y) => x + y)
      .filter(x => x._1 == true)
      .map(x => x._2)
      .take(5)
      .foreach(println)




  }
}
