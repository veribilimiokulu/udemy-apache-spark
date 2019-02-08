package SparkTemel.DistributedVariables
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


    // Satış tutarı bakımından en çok hacme sahip 10 ürün

    /////////////////////////////  VERİ OKUMA products.csv /////////////////////////////////////////////////////////
    //==========================================================================================================

    def loadProducts():Map[Int, String]={
      val source = Source.fromFile("D:\\Datasets\\retail_db\\products.csv")

      val lines = source.getLines()
        .filter(x => (!(x.contains("productCategoryId")))) // başlık filtreleme

      var productIdAndName:Map[Int, String] = Map()
      for (line <- lines){
        val productId = line.split(",")(0).toInt
        val productName = line.split(",")(2)

        productIdAndName += (productId -> productName)
      }

      return  productIdAndName
    }

    val broadcastProduct = sc.broadcast(loadProducts)

    /////////////////////////////  VERİ OKUMA order_items.csv  /////////////////////////////////////////////////////////
    //==========================================================================================================

    // order_items.csv okuma RDD yapma
    val orderItemsRDD = sc.textFile("D:\\Datasets\\retail_db\\order_items.csv")
      .filter(!_.contains("orderItemName")) // İlk başlık satırından kurtulma


    //////////////// OKUNAN VERİLERİ PAIR RDD'ye ÇEVİRME   /////////////////////////////////////////////////////
    //================================================================================================================
    //////////////// order_items için PairRDD oluşturma //////////////////////////////////////////

    def makeOrderItemsPairRDD(line:String)={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId= line.split(",")(1)
      val orderItemProductId= line.split(",")(2).toInt
      val orderItemQuantity= line.split(",")(3)
      val orderItemSubTotal= line.split(",")(4).toFloat
      val orderItemProductPrice = line.split(",")(5)

      (orderItemProductId, orderItemSubTotal)
    }
    // Fonksiyonu kullanarak PairRDD olluşturma

    val orderItemPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)

    orderItemPairRDD.take(6).foreach(println(_))

    println("reducebykey aşaması")
    val sortedOrders = orderItemPairRDD.reduceByKey((x,y) => x+y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      //.take(10).foreach(println(_))

    // Sıralanmış ürün isimleri ile broadcastedproducts'ın ürünid si ile eşleştir.

    val sortedOrdersWithProductName = sortedOrders.map(x => (broadcastProduct.value(x._1), x._2))
    sortedOrdersWithProductName.take(10).foreach(println(_))
  }
}
