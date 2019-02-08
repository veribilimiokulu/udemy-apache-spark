package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
retail_db'deki order_items.orderItemProductId ile  products.productId birleştirme
 */
object Join {
  def main(args: Array[String]): Unit = {
    // Log seviyesi ayarlama: sadece hataları göstersin
    Logger.getLogger("org").setLevel(Level.ERROR)

    /////////////////////////////  SPARKCONTEXT OLUŞTURMA /////////////////////////////////////////////////////////
    //==========================================================================================================
    val conf = new SparkConf().setAppName("Join").setMaster("local[4]")
    val sc = new SparkContext(conf)



    /////////////////////////////  VERİ OKUMA SAFHASI /////////////////////////////////////////////////////////
    //==========================================================================================================
    // order_items.csv okuma
    val orderItemsRDD = sc.textFile("D:\\Datasets\\retail_db\\order_items.csv")
      .filter(!_.contains("orderItemName")) // İlk başlık satırından kurtulma
    println("\norder_items ilk göz atma: ")
    orderItemsRDD.take(5).foreach(println(_))

    // products okuma
    val productsRDD = sc.textFile("D:\\Datasets\\retail_db\\products.csv")
      .filter(!_.contains("productDescription")) // İlk başlık satırından kurtulma
    println("\norder_items ilk göz atma: ")
    productsRDD.take(5).foreach(println(_))





    //////////////// OKUNAN VERİLERİ PAIR RDD'ye ÇEVİRME SAFHASI  /////////////////////////////////////////////////////
    //================================================================================================================
    //////////////// order_items için PairRDD oluşturma //////////////////////////////////////////

    def makeOrderItemsPairRDD(line:String) ={
      val orderItemName = line.split(",")(0)
      val orderItemOrderId = line.split(",")(1)
      val orderItemProductId = line.split(",")(2)
      val orderItemQuantity = line.split(",")(3)
      val orderItemSubTotal = line.split(",")(4)
      val orderItemProductPrice = line.split(",")(5)

      // orderItemProductId anahtar,  kalanlar değer olacak şekilde PairRDD döndürme
      (orderItemProductId, (orderItemName, orderItemOrderId, orderItemQuantity,orderItemSubTotal, orderItemProductPrice))
    }

    // Fonksiyonu kullanarak PairRDD oluşturma
    val orderItemsPairRDD = orderItemsRDD.map(makeOrderItemsPairRDD)
    // oluşan PairRDD'yi görme
    println("\norderItemsPairRDD görme")
    orderItemsPairRDD.take(5).foreach(println(_))


    ///////////////////////// products için PairRDD oluşturma  //////////////////////////////////

    def makeProductsPairRDD(line:String) ={
      val productId = line.split(",")(0)
      val productCategoryId = line.split(",")(1)
      val productName = line.split(",")(2)
      val productDescription = line.split(",")(3)
      val productPrice = line.split(",")(4)
      val productImage = line.split(",")(5)

      (productId, (productCategoryId, productName, productDescription, productPrice, productImage))
    }

    val productsPairRDD = productsRDD.map(makeProductsPairRDD)

    // oluşan yeni PairRDD'ye göz atma
    println("\nproductsPairRDD: ")
    productsPairRDD.take(5).foreach(println(_))




    ////////////////////////////////////////// JOIN AŞAMASI  /////////////////////////////////////////////////////
    //============================================================================================================

    //////////////// PairRDD'ler oluştu. Anahtarların ikisini de productId yaptık. Şimdi Join ////////////////////////
    val orderItemProductJoinedRDD = orderItemsPairRDD.join(productsPairRDD)
    println("\norderItemProductJoinedRDD:")
    orderItemProductJoinedRDD.take(10).foreach(println(_))

    /////// Basit bir kontrol büyük tablo 172.199 satır eğer tüm ürünlerden satış olmuşsa aynı sayı elde edilmeli
    println("orderItemsRDD satır sayısı: " + orderItemsRDD.count())
    println("productsRDD satır sayısı: " + productsRDD.count())
    println("orderItemProductJoinedRDD satır sayısı: " + orderItemProductJoinedRDD.count())
    /*
    orderItemsRDD satır sayısı: 172198
    productsRDD satır sayısı: 1345
    orderItemProductJoinedRDD satır sayısı: 172198
     */


  }
}
