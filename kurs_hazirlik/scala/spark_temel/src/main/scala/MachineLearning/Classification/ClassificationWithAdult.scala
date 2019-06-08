package MachineLearning.Classification

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
object ClassificationWithAdult {
  def main(args: Array[String]): Unit = {

    // Log seviyesini ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession.builder()
      .appName("Logistic Regression Classification")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executormemory","4g")
      .getOrCreate()

    // Veri okuma
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:\\Datasets\\adult_preprocessed.csv")


    //////////////////// 1. Veri Keşfi ////////////////////////////////////////////////////////

    // 1.1. Veri setine göz atma
      df.show()


    //  1.2. Veri seti şemasını görme
      df.printSchema()


    // 1.3. Nitelikleri Nümerik ve Kategorik olarak ayırmak
      // Aşağıda tanımlı fonksiyonu kullanarak kategorik ve nümerik nitelikleri elde etme
      var (kategorik, numerik) = identifyCategoricAndNumeric(df)

    // kategorik içinden hedef değişkeni çıkaralım
      kategorik = kategorik.filter(!_ .contains("output"))

      // kategorik ve nimerik nitelikleriyazdırma
      println("\nKategorik Nitelikler:")
      kategorik.foreach(println(_))
      println("\nNümerik Nitelikler:")
      numerik.foreach(println(_))


    // 1.4. Nümerik Nitelikler ile Betimsel İstatistik
      df.describe(numerik:_*).show()

    // 1.5. Kategorik niteliklerin kategorilerini ve frekans dağılımlarını groupBy kullanarak incelemek
      for (catCol <- kategorik){
        println(s"\n$catCol için groupby")
        df.groupBy(catCol).count().sort(F.desc("count")).show()
      }

    ////////////////////  2. Veri Temizliği ve Ön İşleme //////////////////////////////////////
    // 2.1. Null kontrolü
      println("\nNull kontrolü: ")
      for(col <- df.columns){
        if(df.filter(df.col(col).isNull).count() > 0){
          println(s"$col içinde null VAR")
        }else{
          println(s"$col içinde null YOK")
        }
      }
    // daha önceden temizlediğimiz bir veri olduğu için null değer yok



    // 2.2. StringIndexer, OneHotEncoder ve VectorAssembler için kategorik ön hazırlık
      // Kategorik değişkenler için StringIndexer ve OneHotEncoderları toplayacak boş Array
          var categoricalColsPipeStages = Array[PipelineStage]()

      // Kategorik değişkenler için OneHotEncoder çıktı isimlerini toplayıp VectorAssembler'a
      // verecek boş Array
          var catsForVectorAssembler = Array[String]()

    // StringIndexer, OneHotEncder ve VectorAssembler için for döngüsü yap. Tüm kategorik değişkenleri
    // sırasıyla StringIndexer, OneHotEncoder'dan geçir ve VectorAssembler'a hazır hale getir.
    // Yani Vector Assembler'a hangi sütun isimlerini verecekisek onları bir Array (catsForVectorAssembler)'de topla
    for (col <- kategorik) {

        val stringIndexer = new StringIndexer()
          .setHandleInvalid("skip")
          .setInputCol(col)
          .setOutputCol(s"${col}Index")  // StringIndexer'in ürettiği sütun ismi
        val encoder = new OneHotEncoderEstimator().setInputCols(Array(s"${col}Index"))
          .setOutputCols(Array(s"${col}OneHot")) // OneHotEncoder'ın ürettiği sütun ismi

        // Pipeline için nesneleri bir Array içine topla (her döngüde 1 StringIndexer, 1 OnehotEncoder
        categoricalColsPipeStages = categoricalColsPipeStages :+ stringIndexer :+ encoder
        catsForVectorAssembler = catsForVectorAssembler :+ s"${col}OneHot"
    }

    // 2.3. VectorAssembler aşaması
      val assembler = new VectorAssembler()
        .setInputCols(numerik ++ catsForVectorAssembler)
        .setOutputCol("features")


    // 2.4. Standardizasyon aşaması
      val scaler = new StandardScaler()
        .setInputCol("features")
        .setOutputCol("scaledFeatures")

    // 2.5. LabelEncoder
      val labelEncoder = new StringIndexer()
        .setInputCol("output")
        .setOutputCol("label")

    // 3. MODEL AŞAMASI
      // 3.1. Sınıflandırıcı

        val classifier = new LogisticRegression()
          .setLabelCol("label")
          .setFeaturesCol("scaledFeatures")


      // 3.2. Pipeline oluşturma
        val pipelineObj = new Pipeline()
        .setStages(categoricalColsPipeStages ++ Array(assembler,scaler,labelEncoder, classifier))

      // 3.4. Veriyi eğitim ve test olarak bölme
          val Array(trainDF, testDF) = df.randomSplit(Array(0.8, 0.2), seed = 142L)

      // 3.5. Modeli Eğitme
        val pipelineModel = pipelineObj.fit(trainDF)

    // 4. MODEL DEĞERLENDİRME
      // 4.1. test veri seti ile tahmin değerlerini üretme
        val transformedDF = pipelineModel.transform(testDF)
    transformedDF.show()

      // 4.2. Modes değerlendirme
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setMetricName("accuracy")

        val accuracy = evaluator.evaluate(transformedDF)

      println(accuracy)

  }
  // Fonksiyonlar
      def identifyCategoricAndNumeric(df: DataFrame): (Array[String], Array[String]) = {
        /*
        Bu fonksiyon parametre olarak analiz edilecek dataframe'i alır. String olan nitelikleri kategorik olmayanları da
        nümerik olarak ayırarak ve iki farklı Array[String] döner.
         */
        var categoricCols = Array[String]()
        var numericCols = Array[String]()
        for (col <- df.columns){
          if(df.schema(col).dataType  == StringType){
            categoricCols = categoricCols :+ col
          }else{
            numericCols = numericCols :+ col
          }
        }

        (categoricCols, numericCols)
      }



}
