package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object BasitLineerRegresyon {
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

    //df.show()

    //
    val df2 = df.withColumn("Advertisement", (col("TV") + col("Newspaper") + col("Radio")))
      .withColumnRenamed("Sales","label")
      .drop("TV","Radio","Newspaper")
    //println( "df2: ")
   // df2.show()

    //********* VERİ HAZIRLIĞI ************************


    //********* NÜMERİK DEĞİŞKENLERİN İSTATİSTİKLERİNE GÖZ ATMAK ************************
    // ortalama, standart sapma aykırı değerler incelenir
    df2.describe("Advertisement", "label").show()

    // VERİ ÖN HAZIRLIĞI
    import  org.apache.spark.ml.feature.{VectorAssembler}
    val vectorAssembler = new VectorAssembler()
      .setInputCols(Array("Advertisement"))
      .setOutputCol("features")


    // Regresyon modeli
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val lrObj = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline
    import org.apache.spark.ml.Pipeline
    val pipelineObj = new Pipeline()
      .setStages(Array(vectorAssembler, lrObj))

    // veri setini ayırma
    val Array(trainDF, testDF) = df2.randomSplit(Array(0.8, 0.2), 142L)

    // Modeli Eğitme
    val pipelineModel = pipelineObj.fit(trainDF)

    pipelineModel.stages.foreach(println(_))

    val resultDF = pipelineModel.transform(testDF)

    resultDF.show()

    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    println(s"R kare: ${lrModel.summary.r2}")
    println(s"Düzeltilmiş R kare: ${lrModel.summary.r2adj}")

    println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
    println(s"Katsayılar: ${lrModel.coefficients}")
    println(s"Sabit: ${lrModel.intercept}")
    // p değerlerini görme. Not: Son değer sabit için
    println(s"p değerler: [${lrModel.summary.pValues.mkString(",")}]")
    // t değerlerini görme. Not: Son değer sabit için
    println(s"t değerler: [${lrModel.summary.tValues.mkString(",")}]")

    resultDF.withColumn("residuals", (col("label") - col("prediction"))).show()

    // y = 4.537119328969264 + 0.04723638038563483 * Advertisement

    val predictDF = Seq(100).toDF("Advertisement")
    val dfPredictionsVec = vectorAssembler.transform(predictDF)
    lrModel.transform(dfPredictionsVec).show()

  }
}
