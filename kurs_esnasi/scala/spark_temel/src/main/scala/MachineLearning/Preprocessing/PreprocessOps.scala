package MachineLearning.Preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
object PreprocessOps {
  def main(args: Array[String]): Unit = {
    ///////////////////// LOG SEVİYESİNİ AYARLAMA /////////////////////
    Logger.getLogger("org").setLevel(Level.ERROR)

    ///////////////////// SPARK SESSION OLUŞTURMA /////////////////////
    val spark = SparkSession.builder()
      .appName("StringIndexerOps")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    ///////////////////// VERİ OKUMA ///////////////////////////////////
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:\\Datasets\\simple_data.csv")

    println("Orjinal DF:")
    df.show()


    ///////////////////// VERİ SETİNE ETİKET EKLEME ///////////////////////////////////
    // Sınıflandırma hedef değişken (etiket-label) yaratmak adına
    // Geliri 7000 üstü olanların ekonomik_durumu iyi diyelim.

    val df1 = df.withColumn("ekonomik_durum",
      when(col("aylik_gelir").gt(7000),"iyi")
        .otherwise("kötü")
    )
    println("ekonomik_durum eklenmiş DF:")
    df1.show()


    /////////////////////  StringIndexer AŞAMASI  ////////////////////////////////////////////////
    //=============================================================================================

    val meslekIndexer = new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")

    val meslekIndexerModel = meslekIndexer.fit(df1)
    val meslekIndexedDF = meslekIndexerModel.transform(df1)

    println("meslek sütunu için StringIndex eklenmiş DF: meslekIndexedDF")
    meslekIndexedDF.show()

    println("Mesleklerin frekansları: ")
    df.groupBy(col("meslek"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("meslek"))
      .show()


    ///////////////////// sehir sütunu için StringIndexer ///////////////////////////////////////

    // sehir için bir adet StringIndexer nesnesi oluşturalım.
    val sehirIndexer = new StringIndexer()
      .setInputCol("sehir")
      .setOutputCol("sehirIndex")
      .setHandleInvalid("skip")


    val sehirIndexerIndexModel = sehirIndexer.fit(meslekIndexedDF)
    val sehirIndexedDF = sehirIndexerIndexModel.transform(meslekIndexedDF)

    println("sehir sütunu için StringIndex eklenmiş DF: sehirIndexedDF")
    sehirIndexedDF.show()

    println("Şehirlerin frekansları: ")
    df.groupBy(col("sehir"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("sehir"))
      .show()

    ////////////////// OneHotEncoderEstimator AŞAMASI ////////////////////////////////////////////
    // =============================================================================================

    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array[String]("meslekIndex","sehirIndex"))
      .setOutputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded"))

    val encoderModel = encoder.fit(sehirIndexedDF)
    val oneHotEncodeDF = encoderModel.transform(sehirIndexedDF)

    println("oneHotEncodeDF:")
    oneHotEncodeDF.show()


    ////////////////// VectorAssembler AŞAMASI ////////////////////////////////////////////
    // ====================================================================================

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexEncoded","sehirIndexEncoded","yas","aylik_gelir"))
      .setOutputCol("vectorizedFeatures")

    val vectorAssembledDF = vectorAssembler.transform(oneHotEncodeDF)


    println("vectorAssembledDF: ")
    vectorAssembledDF.show(false)



    ////////////////// LabelIndexer AŞAMASI ////////////////////////////////////////////
    // ====================================================================================
    val labelIndexer = new StringIndexer()
      .setInputCol("ekonomik_durum")
      .setOutputCol("label")

    val labelIndexerModel = labelIndexer.fit(vectorAssembledDF)
    val labelIndexerDF = labelIndexerModel.transform(vectorAssembledDF)

    println("labelIndexerDF: ")
    labelIndexerDF.show(truncate = false)

    ////////////////// StandardScaler AŞAMASI ////////////////////////////////////////////
    // ====================================================================================

    import scala.math._
   val yasinEtkisi = sqrt(pow((35-33),2))
    val maasEtkisi = sqrt(pow((18000-3500),2))
    val oklitMesafesi = sqrt(pow((35-33),2) + pow((18000-3500),2))

    println(s"Toplam etki: $oklitMesafesi, yaş etkisi: $yasinEtkisi, maaş etkisi: $maasEtkisi")

    val scaler = new StandardScaler()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("features")

    val scalerModel = scaler.fit(labelIndexerDF)
    val scalerDF = scalerModel.transform(labelIndexerDF)

    println("scalerDF: ")
    scalerDF.show(truncate = false)


    ////////////////// Train-Test Ayırma AŞAMASI ////////////////////////////////////////////
    // ====================================================================================


   val Array(trainDF, testDF) =  scalerDF.randomSplit(Array(0.8, 0.2), 142L)

    println("trainDF: ")
    trainDF.show(false)
    println("testDF: ")
    testDF.show(false)



  }
}
