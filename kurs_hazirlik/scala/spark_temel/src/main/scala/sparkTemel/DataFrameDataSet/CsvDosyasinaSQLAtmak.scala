package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object CsvDosyasinaSQLAtmak {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("CsvDosyasinaSQLAtmak")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // Dosyadan (veri okumak transformation operasyonudur)
    val dfFromFile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")

    dfFromFile.cache()

    dfFromFile.createOrReplaceTempView("tablo")

    spark.sql(
      """


        SELECT Country, AVG(UnitPrice) AS AvgUnitPrice FROM tablo
        GROUP BY COUNTRY
        LIMIT 10


      """).show(20,false)

    // ilk çalışmada cache yaptığı için ikincisinde daha hızlı sonuç dönecek
    println("\nilk çalışmada cache yaptığı için ikincisinde daha hızlı sonuç dönecek\n")
    spark.sql(
      """


        SELECT Country, AVG(UnitPrice) AS AvgUnitPrice FROM tablo
        GROUP BY COUNTRY
        LIMIT 10


      """).show(20,false)


  }
}
