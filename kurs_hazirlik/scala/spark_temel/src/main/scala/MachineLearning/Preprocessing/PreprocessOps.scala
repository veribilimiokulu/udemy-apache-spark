package MachineLearning.Preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
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
      when(col("aylik_gelir").gt(7000), "iyi")
        .otherwise("kötü"))

    println("ekonomik_durum eklenmiş DF:")
    df1.show()



    /////////////////////  StringIndexer AŞAMASI  ////////////////////////////////////////////////
    //=============================================================================================

    ///////////////////// meslek sütunu için StringIndexer ///////////////////////////////////////
    // meslek için bir adet StringIndexer nesnesi oluşturalım.
    val meslekIndexer = new StringIndexer()
      .setInputCol("meslek")
      .setOutputCol("meslekIndex")
      .setHandleInvalid("skip")// Varsayılan error. Tanımadığı bir değer ile karşılaşınca hata verir.
    // Özellikle zayıf sınıflar her bir "partition" da bulunmayabilir.
    // Hata almamak için skip.


    // meslekIndexer nesnesini eğitelim.
    // Burada aslında çok karmaşık bir eğitim yok. Sadece hangi string değerin kaç kere tekrarlandığını hesaplıyor.
    // sıralıyor ve en çok tekrarlandan en az tekrarlanana doğru 0.0'dan başlayarak ardışık artan tam sayılar atayacak şekilde
    // kendini veriye uyduruyor.
    // Bir çok makine öğrenmesi sınıfında olduğu gibi bu da bir model(StringIndexerModel) üretiyor.
    val meslekndexerIndexModel = meslekIndexer.fit(df1)


    // StringIndexerModel'in transform metoduna orjinal df'i parametre verdiğimizde bize string niteliğin indekslenmiş halini
    // dataframe'e ekleyerek dataframe'i geri dönüyor.
    val meslekIndexedDF = meslekndexerIndexModel.transform(df1)
    println("meslek sütunu için StringIndex eklenmiş DF: meslekIndexedDF")
    meslekIndexedDF.show()


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

    ///////////////////// hedef değişken (ekonomik_durum) için StringIndexer ///////////////////////////////////////
    val labelIndexer = new StringIndexer()
    labelIndexer.setInputCol("ekonomik_durum")
    labelIndexer.setOutputCol("label")

    val labelIndexerModel = labelIndexer.fit(sehirIndexedDF)
    val labelIndexedDF = labelIndexerModel.transform(sehirIndexedDF)
    println("label (ekonomik_durum) sütunu için StringIndex eklenmiş DF: sehirIndexedDF")
    labelIndexedDF.show()


    ////////////////// OneHotEncoderEstimator AŞAMASI ////////////////////////////////////////////
   // =============================================================================================

    // Yeni bir OneHotEncoderEstimator nesnesi oluşturalım.
    val encoder = new OneHotEncoderEstimator()
      .setInputCols(Array("meslekIndex","sehirIndex")) // StringIndexed sütunları topluca ver
      .setOutputCols(Array("meslekIndexVec","sehirIndexVec")) // OneHotEncoded sütun isimlerini topluca ver.

    val myOneHotEncoderModel = encoder.fit(labelIndexedDF)
    val oneHotEncodeDF = myOneHotEncoderModel.transform(labelIndexedDF)

    println("oneHotEncodeDF:")
    oneHotEncodeDF.show()

    println("Şehirlerin frekansları: ")
    df.groupBy(col("sehir"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("sehir"))
      .show()


    println("Mesleklerin frekansları: ")
    df.groupBy(col("meslek"))
      .agg(
        count(col("*")).as("sayi")
      )
      .sort(desc("sayi"), asc("meslek"))
      .show()



    // Peki her nitelik için bunca işi tekrarlayacak mıyız?
    // Cevabı Pipeline'da.


    ////////////////// VectorAssembler AŞAMASI ////////////////////////////////////////////
    // ====================================================================================

    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array[String]("meslekIndexVec","sehirIndexVec","yas","aylik_gelir"))
      .setOutputCol("vectorizedFeatures")

    val vectorAssembledDF = vectorAssembler.transform(oneHotEncodeDF)
    println("vectorAssembledDF: ")
    vectorAssembledDF.show(false)


    ////////////////// StandardScaler AŞAMASI ////////////////////////////////////////////
    // ====================================================================================
    val standardScaler = new StandardScaler()
      .setInputCol("vectorizedFeatures")
      .setOutputCol("features")
      .setWithStd(true) // Whether to scale the data to unit standard deviation.

    val standardScalerModel = standardScaler.fit(vectorAssembledDF)
    val standardScaledDF = standardScalerModel.transform(vectorAssembledDF)
    println("standardScaledDF: ")
    standardScaledDF.show(false)

  }
}
