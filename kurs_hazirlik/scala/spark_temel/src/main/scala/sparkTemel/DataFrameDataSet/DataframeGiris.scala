package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object DataframeGiris {
  def main(args: Array[String]): Unit = {

    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("DataframeOlusturma")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._


    //********* DATAFRAME OLUŞTURMA ************************
    // Listeden
    val dfFromList = sc.parallelize(List(1,2,3,4,5,6)).toDF
    dfFromList.printSchema()

    // range ile
    val rangeDF = spark.range(10,1000,5).toDF("rakamlar")
    rangeDF.take(10).foreach(println(_))

    // Dosyadan (veri okumak transformation operasyonudur)
    val dfFromFile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")

    dfFromFile.printSchema()

    //veya
    val dfFromFile2 = spark.read
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .csv("D:\\Datasets\\OnlineRetail.csv")

    dfFromFile2.printSchema()


    //********* DATAFRAME ÖRNEK TRANSFORMATION VE ACTION ************************

    // count() Action
    println("\nOnlineRetail satır sayısı: "+ dfFromFile.count())


    // select transformation ve show() action
    dfFromFile.select("InvoiceNo","Quantity").show(15)

    // explain(): Bu işi nasıl yapacağını bana bir anlat
    dfFromFile.sort($"Quantity").explain()


    // Dinamik conf ayarı ve shuffle partition sayısını değiştirme
    spark.conf.set("spark.sql.shuffle.partitions","5")


    // Yeni conf ile sort ve ilk üçü görme
    dfFromFile.select("Description","Quantity")
      .sort(dfFromFile.col("Quantity"))
      .take(3).foreach(println(_))



  }
}
