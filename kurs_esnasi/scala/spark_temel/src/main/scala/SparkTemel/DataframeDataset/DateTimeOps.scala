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
      .appName("DateTimeOps")
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
    //df.show(25)

    // Varsayılan format : yyyy-MM-dd HH:mm:ss
    val mevcutFormat = "dd.MM.yyyy HH:mm"
    val formatTR = "dd/MM/yyyy HH:mm:ss"
    val formatENG = "MM-dd-yyyy HH:mm:ss"

    val df2 = df.withColumn("InvoiceDate", F.trim($"InvoiceDate")) // boşlukları temizle
      .withColumn("NormalTarih", F.to_date($"InvoiceDate",mevcutFormat)) // normal formatta sadece tarih
      .withColumn("StandartTS", F.to_timestamp($"InvoiceDate", mevcutFormat)) // normal timestamp
      .withColumn("UnixTS", F.unix_timestamp($"StandartTS"))
      .withColumn("TSTR", F.date_format($"StandartTS", formatTR))
      .withColumn("TSENG", F.date_format($"StandartTS", formatENG))
      .withColumn("BirYil", F.date_add($"StandartTS",365))
      .withColumn("Year", F.year($"StandartTS"))
      .withColumn("Fark",F.datediff($"BirYil", $"StandartTS"))

    df2.show(5)

  }
}
