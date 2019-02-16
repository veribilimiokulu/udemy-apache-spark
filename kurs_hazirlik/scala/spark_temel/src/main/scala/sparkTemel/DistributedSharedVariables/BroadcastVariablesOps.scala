package sparkTemel.DistributedSharedVariables
/*
Erkan ŞİRİN
Ciroya en çok katkıda bulunan 10 ürünü broadcast variables kullanarak bulma
 */
import org.apache.log4j.{Logger, Level}
import org.apache.spark.{SparkContext, SparkConf}
import scala.io.Source
object BroadcastVariablesOps {
  def main(args: Array[String]): Unit = {

    // LOG SEVİYESİNİ AYARLAMA
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SPARKCONTEXT OLUŞTURMA
    val conf = new SparkConf().setAppName("BroadcastVariablesOps").setMaster("local[4]")
    val sc = new SparkContext(conf)

    /////////////////////////////  VERİ OKUMA products.csv /////////////////////////////////////////////////////////
    //==========================================================================================================


    def loadProducts():Map[Int, String] ={

      val source = Source.fromFile("D:\\Datasets\\retail_db\\products.csv")
      val lines = source.getLines()
        .filter(x => !(x.contains("productCategoryId"))) // başlık satırından kurtulma

      var productIdAndNames:Map[Int,String] = Map()
      for(line <- lines){
        val productId = line.split(",")(0).toInt
        val productName = line.split(",")(2)

        productIdAndNames += (productId -> productName)
      }


      return productIdAndNames
    }

    // fonksiyon ile dosyayı okuyalım ve broadcast edelim
    val broadcastedProducts = sc.broadcast(loadProducts)
    broadcastedProducts.value

    /////////////////////////////  VERİ OKUMA order_items.csv  /////////////////////////////////////////////////////////
    //==========================================================================================================

    // order_items.csv okuma RDD yapma
    val orderItemsRDD = sc.textFile("D:\\Datasets\\retail_db\\order_items.csv")
      .filter(!_.contains("orderItemName")) // İlk başlık satırından kurtulma


    //////////////// OKUNAN VERİLERİ PAIR RDD'ye ÇEVİRME   /////////////////////////////////////////////////////
    //================================================================================================================
    //////////////// order_items için PairRDD oluşturma //////////////////////////////////////////

    def makeOrderItemsPairRDD(line:String) ={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2).toInt
      val orderItemQuantity = line.split(",")(3).toInt
      val orderItemSubTotal = line.split(",")(4).toFloat
      val orderItemProductPrice = line.split(",")(5).toFloat

      // orderItemProductId anahtar,  kalanlar değer olacak şekilde PairRDD döndürme
      (orderItemProductId, orderItemSubTotal)
    }

    // Fonksiyonu kullanarak PairRDD oluşturma
    val orderItemsPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)


// Tutar bakımından En çok sipariş edilen
    val sortedOrders = orderItemsPairRDD.reduceByKey((x,y) => x+y)
      .map(x => (x._2, x._1)) // Önce tutarları anahtar yap
      .sortByKey(false) // tutarları büyükten küçüğe sırala
      .map(x => (x._2, x._1)) // tekrar ürün ismi ile tutarları yer değiştir.

// Sıralanmış ürün isimleri ile broadcastedproducts'ın ürünid si ile eşleştir.
    val sortedOrdersWithProName = sortedOrders.map(x => (broadcastedProducts.value(x._1), x._2))
    sortedOrdersWithProName.take(10).foreach(println(_))
  }
}
