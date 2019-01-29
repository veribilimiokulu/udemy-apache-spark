package SparkTemel.DataframeDataset

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{functions => F}

object DateTimeOps {
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

    // veriyi okuyarak dataframe oluşturma. Sadece tarih sütununu ve tekil tarihleri seçelim.
    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")
      .select("InvoiceDate").distinct()

    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df.show(5)

    // Varsayılan format : yyyy-MM-dd HH:mm:ss
    var mevcutFormat = "dd.MM.yyyy HH:mm:ss"
    var yeniFormatTR = "dd/MM/yyyy HH:mm:ss"
    var yeniFormatENG = "MM/dd/yyyy HH:mm:ss"

    val df2 = df.withColumn("InvoiceDate", F.trim($"InvoiceDate")) // boşlukları temizle
      .withColumn("InvoiceDate", F.concat($"InvoiceDate", F.lit(":00"))) // saliseyi ekle
      .withColumn("NormalTarih", F.to_date($"InvoiceDate", mevcutFormat)) //
      .withColumn("StandartTS", F.to_timestamp($"InvoiceDate", mevcutFormat))
      .withColumn("UnixTimeStamp", F.unix_timestamp($"InvoiceDate",mevcutFormat)) // 01 Ocak 1970'den bu ana kadarki milisaniye
      .withColumn("YeniTarihTR", F.date_format($"StandartTS", yeniFormatTR)) // Türk format
      .withColumn("YeniTarihENG", F.date_format($"StandartTS", yeniFormatENG)) // USA format
      .withColumn("Yil",F.year($"StandartTS"))


    println("\n Format sonrası görnüm")
    df2.show(10)
    df2.printSchema()


    println("\n Güncel tarihi görmek")
    spark.range(1).select(F.current_date().as("Bugun")).show()

    println("\n Saati görmek")
    spark.range(1).select(F.current_timestamp().as("Suan")).show(false)

    println("\n Zaman Damgasının Formatını değiştirmek")
    spark.range(1).select(F.current_timestamp().as("Suan"))
      .withColumn("SuanYeniFormatTR", F.date_format($"suan", yeniFormatTR)) // Türk format
      .withColumn("SuanYeniFormatENG", F.date_format($"suan", yeniFormatENG)) // USA format
      .show(false)


    // Gün ekleme
    val df3 = df2.withColumn("BirYilSonrasi", F.date_add(F.col("StandartTS"), 365))

    // Tarih farkı. Günler eklenmiş mi?
    println("\n Tarih farkı")
    df3.select(F.datediff(F.col("BirYilSonrasi"), F.col("StandartTS")).as("TarihFarki")).show(5)
  }
}
