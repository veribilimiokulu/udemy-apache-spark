package sparkTemel.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataCleaning {
  def main(args: Array[String]): Unit = {

    // Log seviyesi ayarı
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    //sparkContext oluşturma
    val sc = spark.sparkContext

    // Veri okuma
    val retailRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetail.csv")

    // Okunan veriye göz atma
    //retailRDD.take(5).foreach(println)

    /***************** HEADER'DAN KURTULMA  *****************************/
    val firstline = retailRDD.first().split(";").toArray
    // firstline: Array[String] = Array(InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)

    val firstlinerdd = sc.parallelize(firstline.toSeq)
    //firstlinerdd: org.apache.spark.rdd.RDD[String] = ParallelCollectionRDD[5] at parallelize at <console>:27

    val retailRDDWithoutHeader = retailRDD.subtract(firstlinerdd)
  // retailRDDWithoutHeader: org.apache.spark.rdd.RDD[String] = MapPartitionsRDD[9] at subtract at <console>:27

    println()
    //retailRDDWithoutHeader.take(5).foreach(println)


    /***************** VERİ TEMİZLİĞİ  *****************************/
  def verimiTemizle(line:String):String={
    // InvoiceNo;StockCode;Description;Quantity;InvoiceDate;UnitPrice;CustomerID;Country
    var sonuc = ""
    // TRIMMING
    var InvoiceNo = line.split(";")(0).trim
    var StockCode = line.split(";")(1).trim
    var Description = line.split(";")(2).trim
    var Quantity = line.split(";")(3).trim
    var InvoiceDate = line.split(";")(4).trim
    var UnitPrice = line.split(";")(5).trim
    var CustomerID = line.split(";")(6).trim
    var Country = line.split(";")(7).trim

    // CHECK NULL
    InvoiceNo = if(InvoiceNo.isEmpty) "000000" else InvoiceNo
    StockCode = if(StockCode.isEmpty) "000000" else StockCode
    Description = if(Description.isEmpty) "000000" else Description
    Quantity = if(Quantity.isEmpty) "000000" else Quantity
    InvoiceDate = if(InvoiceDate.isEmpty) "000000" else InvoiceDate
    UnitPrice = if(UnitPrice.isEmpty) "000000" else UnitPrice
    CustomerID = if(CustomerID.isEmpty) "000000" else CustomerID
    Country = if(Country.isEmpty) "000000" else Country


    sonuc = InvoiceNo+";"+StockCode+";"+Description+";"+Quantity+";"+InvoiceDate+";"+UnitPrice+";"+CustomerID+";"+Country
    sonuc
  }

    val temizRetailRDD = retailRDDWithoutHeader.map(x => verimiTemizle(x))

    println()
    //temizRetailRDD.take(5).foreach(println)

    /* Boş sayılar
    println("InvoiceNo: " + temizRetailRDD.filter(x => x.split(";")(0) == "000000").count()) // InvoiceNo: 0
    println("StockCode: " + temizRetailRDD.filter(x => x.split(";")(1) == "000000").count()) // StockCode: 0
    println("Description: " + temizRetailRDD.filter(x => x.split(";")(2) == "000000").count()) // Description: 1454
    println("Quantity: " + temizRetailRDD.filter(x => x.split(";")(3) == "000000").count()) // Quantity: 0
    println("InvoiceDate: " + temizRetailRDD.filter(x => x.split(";")(4) == "000000").count()) // InvoiceDate: 0
    println("UnitPrice: " + temizRetailRDD.filter(x => x.split(";")(5) == "000000").count()) // UnitPrice: 0
    println("CustomerID: " + temizRetailRDD.filter(x => x.split(";")(6) == "000000").count()) // CustomerID: 135080
    println("Country: " + temizRetailRDD.filter(x => x.split(";")(7) == "000000").count()) // Country: 0
    */

    // Miktarı 10'dan küçük olanları filtrele
    temizRetailRDD.filter(x => x.split(";")(3).toInt < 10).take(10).foreach(println)

    temizRetailRDD.coalesce(1).saveAsTextFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\Datasets\\OnlineRetailClean")
  }
}
