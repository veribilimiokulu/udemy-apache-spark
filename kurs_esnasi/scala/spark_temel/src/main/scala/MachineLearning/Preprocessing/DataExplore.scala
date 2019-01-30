package MachineLearning.Preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataExplore {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("DataExplore")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    //********* VERİ SETİNİ OKUMA ÖNCESİ KONTROLLER ************************
    // 1. Veri setine ulaşma
    //    adult.data ve adult.test adında iki veri dosyası var
    // 2. Mümkünse notepad++ içine bakalım. Baktık. virgülle ayrılmış ve başlıkları olan bir dosya


    //********* VERİ SETİNİ OKUMA  ************************
    // adult.data veri setini okuma
    val adultTrainDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\adult.data")


    // adult.test veri setini okuma
    val adultTestDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\adult.test")

    // okunan dataframe'e ilk bakış
    println("\n adultTrainDF")
    adultTrainDF.show(5)

    // okunan dataframe'e ilk bakış
    println("\n adultTestDF")
    adultTestDF.show(5)

    val adultWholeDF = adultTrainDF.union(adultTestDF)
    adultWholeDF.show(5)

    // Elimizde şuan üç DF var
    println("adultTrainDF satır sayısı: ")
    println(adultTrainDF.count())
    println("adultTestDF satır sayısı: ")
    println(adultTestDF.count())
    println("adultWholeDF satır sayısı: ")
    println(adultWholeDF.count())


    //********* VERİ SETİNİ İNCELEME ŞEMA İLE KARŞILAŞTIRMA  ************************
    // adultWholeDF şeması: Spark'ın çıkarımda bulunduğu şema ile DF'i kontrol edelim.
    println("adultWholeDF şeması: ")
    adultWholeDF.printSchema()


    //////////////////////  NÜMERİK DEĞİŞKENLERİN İSTATİSTİKLERİ ////////////////////////////////////////////
    // Nümerik değişkenlerin istatistiklerini görelim
    println("Nümerik değişkenlerin istatistiklerini görelim")
    adultWholeDF.describe("fnlwgt","education_num","capital_gain","capital_loss","hours_per_week")
  }
}
