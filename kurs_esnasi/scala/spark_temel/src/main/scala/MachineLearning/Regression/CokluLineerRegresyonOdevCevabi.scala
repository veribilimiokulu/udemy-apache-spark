package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.ml.{Pipeline, PipelineModel}


object CokluLineerRegresyonOdevCevabi {
  def main(args: Array[String]): Unit = {
    ////// Log Seviyesi Ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)

    ////// SparkSession oluşturma
    val spark = SparkSession.builder
      .appName("CokluLineerRegresyonOdevCevabi")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()


    ///// Veriyi okuma
    // Veri kaynağı: https://www.kaggle.com/kumarajarshi/life-expectancy-who
    val df = spark.read.format("csv")
      .option("inferSchema", true)
      .option("header", true)
      .option("sep", ",")
      .load("D:\\Datasets\\LifeExpectancyData.csv")

    //df.show()

    // Nitelik isimlerini değiştir. Sıralamayı bozmadan.
    var newCols = Array("Country", "Year", "Status", "label", "AdultMortality",
      "InfantDeaths", "Alcohol", "PercentageExpenditure", "HepatitisB", "Measles", "BMI", "UnderFiveDeaths",
      "Polio", "TotalExpenditure", "Diphtheria", "HIV_AIDS", "GDP", "Population", "Thinness11", "Thinness59",
      "IncomeCompositionOfResources", "Schooling")

    val df2 = df.toDF(newCols:_*)

    //df2.show()
    //df2.printSchema()

    var categoricCols = Array("Country","Status")
    var numericalCols = Array("Year", "AdultMortality",
      "InfantDeaths", "BMI", "UnderFiveDeaths",
      "TotalExpenditure",  "HIV_AIDS",
      "IncomeCompositionOfResources", "Schooling")


    var label = Array("label")

   // df2.describe().show()

    //veri temizliği
    val df3 = df2.na.drop()

   // df3.describe().show()

    // Veri hazırlığı
    //StringIndexer
    val statusStringIndexer = new StringIndexer().setInputCol("Status").setOutputCol("StatusIdexed")

    //OneHotEncoder
    val encoder = new OneHotEncoderEstimator().setInputCols(Array("StatusIdexed")).setOutputCols(Array("statusEncoded"))

    // VectorAssembler
    val vectorAssembler = new VectorAssembler()
      .setInputCols(numericalCols ++ encoder.getOutputCols)
      .setOutputCol("features")


    //lineer model
    val linearRegressionObject = new LinearRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")

    // pipeline model
    val pipelineObject = new Pipeline().setStages(Array(statusStringIndexer,encoder,vectorAssembler, linearRegressionObject ))


    // veri setini ayırme
    val Array(trainDF, testDF) = df3.randomSplit(Array(0.8, 0.2), 142L)
    trainDF.cache()
    testDF.cache()

    // pipeline ile modeli eğitme
    val pipelineModel = pipelineObject.fit(trainDF)

    pipelineModel.transform(testDF).select("label","prediction").show()

    // pipeline model içinden lineer modeli alma

    val lrModel = pipelineModel.stages.last.asInstanceOf[LinearRegressionModel]
    // Model istatistiklerini görmme
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
    println("lrModel parametreleri: ")
    println(lrModel.explainParams)

    var pIcinNitelikler = numericalCols ++ Array("Status") ++ Array("sabit")
    var zippedPValues = pIcinNitelikler.zip(lrModel.summary.pValues)

    zippedPValues.map(x => (x._2, x._1)).sorted.foreach(println(_))

  }
}
