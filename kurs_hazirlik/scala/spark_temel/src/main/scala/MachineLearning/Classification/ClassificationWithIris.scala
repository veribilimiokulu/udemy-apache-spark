package MachineLearning.Classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.{PipelineModel, Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler}

object ClassificationWithIris {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession.builder()
      .appName("ClassificationWithIris")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executormemory","4g")
      .getOrCreate()

    // Veri okuma
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:\\Datasets\\iris.csv")



    /////////////////////////  1. Veri keşfi  /////////////////////////
    df.show()

    df.describe().show()
    // Null değer yok, aykırı değer gözlenmedi

    // Hedef değişken Species'e göre inceleme dağılımı, sınıf dengesi
    df.groupBy("Species").count().show()


    ///////////////////////// 2. Veri Temizliği ve Ön Hazırlığı /////////////////////////
    // 2.1. Imputer Aşaması
    // Boş nümerik nitelik olmadığı için bu aşamayı uygulamıyoruz

    // 2.2. Kategorik Nitelikler için StringIndexer (labelIndexer) Aşaması
    // Kategorik değişken olmadığı için kategorik değişken işleme ihtiyacı yok
    // Sadece hedef değişkeni StringIndexer ile kodlamamız gerekir.

    import org.apache.spark.ml.feature.StringIndexer

    val stringIndexer = new StringIndexer()
      .setHandleInvalid("skip")
      .setInputCol("Species")
      .setOutputCol("label")

    val labelDF = stringIndexer.fit(df).transform(df)
    labelDF.show()


    // 2.3. OneHotEncoderEstimator Aşaması
    // Kategorik girdi niteliğimiz olmadığı için uygulamıyoruz.
    // Hedef değişken için OneHot yapmıyoruz.

    // 2.4. Vector Assembler Aşaması
    // Girdi nitelikleri tek bir vektör haline getirme

    import  org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"))
      .setOutputCol("features")

    val vectorDF = assembler.transform(labelDF)
    vectorDF.show()

    // 2.5. StandardScaler Aşaması
    // Girdi nitelikler aynı ölçekte (santimetre) olduğu için kullanmamıza gerek yok

    // 2.6. Veriyi train ve test olarak ikiye bölme
    val Array(trainDF, testDF) = vectorDF.randomSplit(Array(0.8, 0.2), seed = 142L)

    ///////////////////////// 3. Model Oluşturma /////////////////////////

    // 3.1. Sınıflandırıcı nesnesi yaratma
    import org.apache.spark.ml.classification.LogisticRegression

    val logregObj = new LogisticRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // 3.2. Nesneyi veri ile eğiterek model elde etme

    val logregModel = logregObj.fit(trainDF)

    // 3.3. Modeli kullanarak tahminde bulunma

    val transformedDF = logregModel.transform(testDF)

    transformedDF.show()


    ///////////////////////// 4. Model Değerlendirme /////////////////////////

    // 4.1. Değerlendirici import ve nesne yaratma
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(transformedDF)

    println("accuracy: ",accuracy)


  }
}
