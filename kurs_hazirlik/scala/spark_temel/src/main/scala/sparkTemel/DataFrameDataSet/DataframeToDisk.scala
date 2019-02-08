package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._



object DataframeToDisk {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("Schema")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._


    // veriyi okuyarak dataframe oluşturma
    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Udemy_Spark_Kursu\\kodlar\\data\\simple_dirty_data.csv")


    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df.show(20)


    // düzeltmeler
    val df2 = df
      .withColumn("isim", trim(initcap($"isim"))) // ismin boşluklarını temizle ilk harfini büyük yap
      .withColumn("cinsiyet", when($"cinsiyet".isNull,"U").otherwise($"cinsiyet")) // cinsiyet null ile U değilse kendi
      .withColumn("sehir",
          when($"sehir".isNull,"BİLİNMİYOR") // sehir de null varsa BİLİNMİYOR yaz
          .otherwise($"sehir")) // değilse aynısını yaz
    .withColumn("sehir",trim(upper($"sehir"))) // sehir boşlukları temizle ve büyük harf yap


    // temizlenmiş dataframe'i diske yaz
    df2
      .coalesce(1) // tek parça olması için
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("D:\\Datasets\\simple_dirty_data")


    // yazdığımız yerden tekrar oku ve gör
    val df3 = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\simple_dirty_data")

    df3.show(15)

  }
}
