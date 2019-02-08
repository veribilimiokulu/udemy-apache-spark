package sparkTemel.RDD
/*
ReduceByKey ile Her bir ürünün sahip olduğu en düşük/yüksek fiyatı bulmak

 */
import org.apache.spark.SparkContext
import org.apache.log4j.{Level,Logger}
object ReduceByKey {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkContext oluştur
    val sc = new SparkContext("local[4]","ReduceByKey-Min-Max")

    // İlk satırdan kurtularak veri oku ve RDD yarat
    val retailRDD = sc.textFile("D:\\Udemy_Spark_Kursu\\data\\OnlineRetail.csv")
      .filter(!_.contains("InvoiceNo"))

    // RDD'nin beş elemanını görelim
    retailRDD.take(5).foreach(println)


    println("\nHer bir ürünün sahip olduğu en düşük/yüksek fiyatı bulmak: ")

    // Stok numarası ve fiyatı ayıklayan fonksiyon
    def getStockNoAndPrice(line:String)={
      val stockNo = line.split(";")(1)
      val price = line.split(";")(5).replace(",",".").toFloat

      (stockNo,price)
    }

    // stokNo ve fiyata sahip RDD map() içine yukarıdaki fonksiyonu vermek yeterli
    val stockAndPriceRDD = retailRDD.map(getStockNoAndPrice)

    // stokNo ve fiyattan oluşan yeni RDD'ye göz atalım
    stockAndPriceRDD.take(5).foreach(println)

    println("\nEn düşük fiyatlar: ")
    // Ürünlerin en düşük fiyatlarına sahip RDD (Aynı ürün farklı tarihlerde farklı fiyattan satılmış olabilir)
    val stockAndPriceRBK = stockAndPriceRDD.reduceByKey((x,y) => (scala.math.min(x,y)))
    stockAndPriceRBK.take(10).foreach(println)


    println("\nEn yüksek fiyatlar: ")
    // Ürünlerin en yüksek fiyatlarına sahip RDD (Aynı ürün farklı tarihlerde farklı fiyattan satılmış olabilir)
    val stockAndPriceRBK2 = stockAndPriceRDD.reduceByKey((x,y) => (scala.math.max(x,y)))
    stockAndPriceRBK2.take(10).foreach(println)

  }
}
