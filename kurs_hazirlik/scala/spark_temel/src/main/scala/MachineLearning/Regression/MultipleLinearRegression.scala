package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object MultipleLinearRegression {
  def main(args: Array[String]): Unit = {


    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)



    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("LinearRegression")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._




    //********* VERİ SETİNİ OKUMA ************************
    // veriyi okuyarak dataframe oluşturma
    // Veri hakkında kısa bilgi: Bir ürünün satış miktarında kullanılan reklam bütçesine ait 200 adet veri
    // Veri kaynağı: https://www.kaggle.com/ishaanv/ISLR-Auto#Advertising.csv
    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\Advertising.csv")


    //********* VERİ SETİNİ ANLAMAK VE KEŞFETMEK ************************
    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df.show(20)

    // Okuma kontrolü yapıldıktan sonra veri seti Kaggle'dan daha detaylı incelenir
    // Veri kaynağı: https://www.kaggle.com/ishaanv/ISLR-Auto#Advertising.csv






    //********* VERİ HAZIRLIĞI ************************
    //********* SÜTUN İSİMLERİNDEKİ BOŞLUKLARI KALDIRMAK VE YENİDEN İSİMLENDİRMEK ************************


    // LifeExpectancy hedef değişken olduğu için onu label yapalım
    val yeniSutunIsimleri = Array("id", "TV", "Radio", "Newspaper", "label")

    // yeni sütun isimleri ile yeni bir dataframe oluşturma
    val df2 = df.toDF(yeniSutunIsimleri:_*)

    // yeni sütun isimlerini ve değerlerin doğruluğunu görelim
    println("Yeni sütün isimleriyle df, Sales label oldu: ")
    df2.show()



    //********* NİTELİKLERİ KATEGORİK, NÜMERİK ve HEDEF NİTELİK OLARAK BELİRLEMEK ************************
    // Kategorik nitelikleri belirleyelim : Yok
   // var kategorikNitelikler = Array()

    // Hedef değişkeni belirleyelim
    var label = Array("label")


    // Nümerik nitelikleri belirleyelim. Bunun için tüm niteliklerden kategorikleri çıkarmalıyız.
    // Önce Set haline çevirdik diff ile iki küme farkını aldık ve sonuu tekrar array haline getirdik.
    var numerikNtelikler = Array("TV","Radio")

    // Kategorik ve nümerik nitelikleri yazdırıp görelim
    println("\nNümerik nitelikler:")
    numerikNtelikler.foreach(println(_))
    //println("\nKategorik nitelikler:")
    //kategorikNitelikler.foreach(println(_))
    println("\nHedef nitelik:")
    label.foreach(println(_))
    println("\nToplam nitelik sayısı:" + df2.columns.length)




    //********* NÜMERİK DEĞİŞKENLERİN İSTATİSTİKLERİNE GÖZ ATMAK ************************
    // ortalama, standart sapma aykırı değerler incelenir
    df2.describe("TV", "Radio", "Newspaper", "label").show()

    //********* KATEGORİK DEĞİŞKENLERİN İSTATİSTİKLERİNE GÖZ ATMAK ************************
    //df2.describe().show()

    // Veri setinde null kontrolü
    println("Null kontrolü: ")
    for(nitelik <- df2.columns){
      if(df2.filter(df2.col(nitelik).isNull).count() > 0) println(s"${nitelik} null") else println(s"${nitelik} içinde null yok")
    }

    // 1. Yöntem : Çok uğraşmadan null değerleri düşürelim
    val df3 = df2.na.drop()



    //*********  DOĞRUSAL REGRESYONUN VARSAYIMLARI KARŞILANIYOR MU?************************


    // ********  NÜMERİK DEĞİŞKENLERİN ÖN HAZIRLIĞI ************************************

    import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler, StandardScaler}

    // Status kategorik değişkeni StringIndexer ile nümerik yapalım
   /* val statusStringIndexer = new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol("Status")
      .setOutputCol("StatusStrIndexed")

    // Status kategorik değişkeni one hot encoder
    val oneHotEncoder = new OneHotEncoderEstimator()
      .setInputCols(Array("StatusStrIndexed"))
      .setOutputCols(Array("StatusOneHotEncoded"))

*/
    //Vector Assembler ile tüm girdileri bir vektör haline getirelim
    val vectorAssembler = new VectorAssembler()
      .setInputCols(numerikNtelikler)
      .setOutputCol("features")

    /*// StandardScaler
    val scaler = new StandardScaler()
      .setInputCol("featuresEnc")
      .setOutputCol("features")
      .setWithMean(true)
*/
    // Regresyon Modeli oluşturma
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val linearRegressionObject = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")


    // Pipeline oluşturma
    import org.apache.spark.ml.Pipeline
    val pipelineObject = new Pipeline()
      .setStages(Array(vectorAssembler, linearRegressionObject))

    // Veri setini train ve test olarak ayırma
    val Array(trainDF, testDF) = df3.randomSplit(Array(0.75, 0.25),142L)

    // Modeli eğitme
    val pipelineModel = pipelineObject.fit(trainDF)

    // Artıkları kendimiz hesaplayıp tahmin, gerçek değer ile yan yana inceleyelim
    val resultDF = pipelineModel.transform(testDF)
    resultDF.withColumn("residuals", (resultDF.col("label") - resultDF.col("prediction"))).show()

    // Pipeline model içinden lrModeli alma
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel] // pipeline katarında lrModel ensonda


    // Regresyon modele ait  istatistikler
      println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
      println(s"R kare : ${lrModel.summary.r2}")
      println(s"Düzeltilmiş R kare : ${lrModel.summary.r2adj}")
      // Değişken katsayılarını görme. Son değer sabit
      println(s"Katsayılar : ${lrModel.coefficients}")
      println(s"Sabit : ${lrModel.intercept}")
      // p değerlerini görme. Not: Son değer sabit için
      println(s"p değerler: [${lrModel.summary.pValues.mkString(",")}]")
      // t değerlerini görme. Not: Son değer sabit için
      println(s"t değerler: [${lrModel.summary.tValues.mkString(",")}]")
      // Regresyon denklem.: y =

      // Dataframe içinde tahmin edilen değerlerle gerçekleri görelim
      //lrModel.summary.predictions.show()



  }

}
