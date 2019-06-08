package MachineLearning.Classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{SparkSession}
object ClassificationWithIris {
  def main(args: Array[String]): Unit = {

    // Log seviyesi ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)

    //SparkSession
    val spark = SparkSession.builder()
      .appName("ClassificationWithIris")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    //veri okuma
    val df = spark.read.format("csv")
      .option("header",true)
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:/Datasets/iris.csv")

    /////////////////////////  1. Veri Keşfi  /////////////////////////////
    df.show()

    df.describe().show()


    df.groupBy("Species").count().show()

    df.printSchema()

///////////////////////////  2. Veri temizliği ve ön hazırlığı   //////////////////////
    // 2.1 Imputer (Sayısal değerlerdeki boşlukları doldurma)

      // Boş nitelik olmadığı için kullanlmadı

    // 2.2. Kategorik Nitelikler için StringIndexer ve OneHotEncoder

      // Kategorik nitelik olmadığı için kullanılmadı
      // Sadece hedef değişken (Species) için gerekecek

    import  org.apache.spark.ml.feature.StringIndexer

    val stringIndexer = new StringIndexer()
      .setInputCol("Species")
      .setOutputCol("label")
      .setHandleInvalid("skip")

    val labelDF = stringIndexer.fit(df).transform(df)
    println("labelDF:")
    labelDF.show()


    // 2.3. VectorAssembler aşaması
    import  org.apache.spark.ml.feature.VectorAssembler

    val assembler = new VectorAssembler()
      .setInputCols(Array("SepalLengthCm","SepalWidthCm","PetalLengthCm","PetalWidthCm"))
      .setOutputCol("features")

    val vectorDF = assembler.transform(labelDF)
    println("vectorDF")
    vectorDF.show()

    // 2.5. Standardizasyon
      // Nitelikler aynı ölçekte olduğu için kullanmıyoruz

    // 2.6. Veriyi train ve test olarak ayırma

    val Array(trainDF, testDF) = vectorDF.randomSplit(Array(0.8, 0.2), seed = 142L)

    ///////////////////  3. Model Oluşturma Aşaması   ////////////////////////////////
    //3.1. Sınıflandırıcı nesnesi yaratma

    import org.apache.spark.ml.classification.LogisticRegression

    val logregObj = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    //3.2. Sınıflandırıcı nesneyi veri ile eğiterek model elde etme
    val logregModel = logregObj.fit(trainDF)

    //3.3. Modeli kullanarak tahmin yapma
    val transformedDF = logregModel.transform(testDF)

    println("transformedDF")
    transformedDF.show(false)

    ///////////////////  4. Model Değerlendirme Aşaması   ////////////////////////////////

    // 4.1. Değerlendirici kütüphane indirme ve nesne oluşturma
    import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")
      .setLabelCol("label")
      .setPredictionCol("prediction")

    val accuracy = evaluator.evaluate(transformedDF)

    println("accuracy", accuracy)


  }
}
