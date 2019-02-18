package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object SimpleLinearRegression {
  def main(args: Array[String]): Unit = {


    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)



    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("LinearRegression")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","6g")
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


    // Basit regresyon yapacağımız için TV, Newspaper ve Radio bütçelerini Advertisement altında birleştirme
    val df2 = df.withColumn("Advertisement", (col("TV") + col("Newspaper") + col("Radio")))
      .withColumnRenamed("Sales","label")
        .drop("TV", "Radio", "Newspaper")

    df2.show(200)
    //********* VERİ SETİNİ ANLAMAK VE KEŞFETMEK ************************
    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df.show(20)

    // Okuma kontrolü yapıldıktan sonra veri seti Kaggle'dan daha detaylı incelenir
    // Veri kaynağı: https://www.kaggle.com/ishaanv/ISLR-Auto#Advertising.csv


    //********* VERİ HAZIRLIĞI ************************


    //********* NÜMERİK DEĞİŞKENLERİN İSTATİSTİKLERİNE GÖZ ATMAK ************************
    // ortalama, standart sapma aykırı değerler incelenir
    df2.describe("Advertisement", "label").show()

    //********* KATEGORİK DEĞİŞKENLERİN İSTATİSTİKLERİNE GÖZ ATMAK ************************
    //Kategorilerin dağılımı, zayıf kategoriler ve hedef değişkene ilişkileri incelenir


    //********* NULL KONTROLÜ ************************
    // Veri setinde null kontrolü
    // for döngüsü ile her bir nitelik için bütün nitelikleri sırayla dolaşarak null kontrolü yap olanları yazdır.
    println("\nNull kontrolü: ")
    for(nitelik <- df2.columns){
      if(df2.filter(df2.col(nitelik).isNull).count() > 0) println(s"${nitelik} null")
    }


    // 1. Yöntem : Çok uğraşmadan null değerleri düşürelim
    val df3 = df2.na.drop()

    // Imputation kullanılabilir

    // df3.show()

    //*********  DOĞRUSAL REGRESYONUN VARSAYIMLARI KARŞILANIYOR MU?************************


    // ********  VERİ ÖN HAZIRLIĞI ************************************

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
      // nümerik değişkenleri daha önce ayırmıştık arkasıa kategorik niteliği ekliyoruz
      .setInputCols(Array("Advertisement"))
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


    // Pipeline model içinden lrModeli alma
    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel] // pipeline katarında lrModel ensonda

    val resultDF = pipelineModel.transform(testDF)

    println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
    println(s"R kare : ${lrModel.summary.r2}")
    println(s"Düzeltilmiş R kare : ${lrModel.summary.r2adj}")
    println(s"Katsayılar : ${lrModel.coefficients}")
    println(s"Sabit : ${lrModel.intercept}")
    // p değerlerini görme. Not: Son değer sabit için
    println(s"p değerler: [${lrModel.summary.pValues.mkString(",")}]")
    // t değerlerini görme. Not: Son değer sabit için
    println(s"t değerler: [${lrModel.summary.tValues.mkString(",")}]")
    // Regresyon denklem.: y = 4.494440665680972 + 0.047522137330645156 * Reklam


    // Artıkları kendimiz hesaplayıp tahmin, gerçek değer ile yan yana inceleyelim
    resultDF.withColumn("residuals", (resultDF.col("label") - resultDF.col("prediction"))).show()

    // y = 4.537119328969264 + 0.04723638038563483 * Advertisement


    // Predictions
    val predictDF = Seq(100).toDF("Advertisement")
    val dfPredictionsVec = vectorAssembler.transform(predictDF)
    lrModel.transform(dfPredictionsVec).show()

  }

}
