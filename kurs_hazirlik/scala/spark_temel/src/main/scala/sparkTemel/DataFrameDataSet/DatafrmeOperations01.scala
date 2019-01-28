package sparkTemel.DataFrameDataSet
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

object DatafrmeOperations01 {
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


    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\simple_data.csv")

    println("\nDataframe ilk görünüş:")
    df.show(5)

    // Şema
    df.printSchema()

    // Yaşa göre
    println("\nYaşa göre artan sıralama: ")
    df.sort(df.col("yas")).show()

    // Gelire göre azalan sıralama
    println("\nGelire göre azalan sıralama: ")
    df.sort($"aylik_gelir".desc).show()



    // Ortalama geliri bulma
    println("\nOrtalama gelir: ")
    import org.apache.spark.sql.functions.{mean,count}
    df.select(mean("aylik_gelir").as("ortalama_aylik_gelir")).show()



    // mesleklere göre ortalama geliri ve mesleğe mensup kişi sayısı
    println("\nmesleklere göre ortalama gelir ve mesleğe mensup kişi sayısı: ")
    df.select("meslek","aylik_gelir")
      .groupBy("meslek")
      .agg(mean("aylik_gelir").as("ortalama_gelir"),
           count("meslek").as("sayi"))
      .show()

//








  }
}
