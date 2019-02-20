package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.Row
object RegressionTuning {
  def main(args: Array[String]): Unit = {

    ////// Log Seviyesi Ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)

    ////// SparkSession oluşturma
    val spark = SparkSession.builder
      .appName("RegressionTuning")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()


    ///// Veriyi okuma
    val df = spark.read.format("csv")
      .option("inferSchema",true)
      .option("header",true)
      .option("sep",",")
      .load("D:\\Datasets\\LifeExpectancyData.csv")

    //df.show()
// Nitelik isimlerini değiştir
    var newCols = Array("Country","Year", "Status","label","AdultMortality",
  "InfantDeaths","Alcohol","PercentageExpenditure" ,"HepatitisB", "Measles","BMI","UnderFiveDeaths",
  "Polio","TotalExpenditure", "Diphtheria", "HIV_AIDS", "GDP", "Population","Thinness11","Thinness59",
  "IncomeCompositionOfResources","Schooling")

    val df2 = df.toDF(newCols:_*)

    var categoricalCols = Array("Country","Status")

    var numericCols = Array("Year", "AdultMortality", "InfantDeaths",
       "BMI","UnderFiveDeaths", "TotalExpenditure", "Diphtheria",
      "HIV_AIDS", "IncomeCompositionOfResources","Schooling")

    var label = Array("label")

    //df.describe().show()
    // satır sayıları farklı demekki null değerler var

    // En az bir nitelikte null varsa onu düşürelim
    val df3 = df2.na.drop()

    //df3.show()
    //df3.describe().show()

    // satır sayıları eşitlendi diğer null değerler kategorik niteliklerle ilgilidir.

    // Veri hazırlığı


    // vectorAssembler
   val vectorAssembler = new VectorAssembler()
     .setInputCols(numericCols)
     .setOutputCol("features")

    // lineer model
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val lrObj = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline model
    val pipelineObj = new Pipeline().setStages(Array(vectorAssembler, lrObj))

    // veri setini ayırma
    val Array(trainDF, testDF) = df3.randomSplit(Array(0.8, 0.2), 142L)
    trainDF.cache()

    // Modeli eğitme
    val pipelineModel = pipelineObj.fit(trainDF)

    // Modeli test etme
    val resultDF = pipelineModel.transform(testDF)

    // test verisi ile yapılan tahminleri görme
    //resultDF.select("label","prediction").show()

    // PipelineModel içinden lineer modelialma
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


    /////  MODEL OLUŞTURMA: GERİYE DOĞRU ELEME YÖNTEMİ ///////////////////
    //===========================================================================
      //p değerlerini daha iyi görmek için
      println(s"p değerleri ile nitelikler:")
      var pIcinNitelikler = numericCols ++ Array("sabit")
      var zippedPValues = pIcinNitelikler.zip(lrModel.summary.pValues)
      zippedPValues.map(x => (x._2, x._1)).sorted.foreach(println(_))


    ///////////////////////////////// MODEL TUNING /////////////////////////////////////////////////
    //================================================================================================

    // Kullanılacak parametreler
    val paramGrid = new ParamGridBuilder()
      .addGrid(lrObj.elasticNetParam, Array(0.2, 0.5, 0.7))
      .addGrid(lrObj.regParam, Array(0.1, 0.01))
      .build()

    // Cross Validation
    val cv = new CrossValidator()
      .setEstimator(pipelineObj)
      .setEvaluator(new RegressionEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)  // Use 3+ in practice
      .setParallelism(2)  // Evaluate up to 2 parameter settings in parallel

    // CV Modeli eğitme
    val cvModel = cv.fit(trainDF)

    //cvModel.transform(testDF).show()

   // cv model içinden best modeli oradan da lineer modeli alma
    println("Best Model")
    val tunedLinearModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[LinearRegressionModel]
    // Regresyon modele ait  istatistikler
    println(s"RMSE: ${tunedLinearModel.summary.rootMeanSquaredError}")
    println(s"R kare : ${tunedLinearModel.summary.r2}")
    println(s"Düzeltilmiş R kare : ${tunedLinearModel.summary.r2adj}")
    // Değişken katsayılarını görme. Son değer sabit
    println(s"Katsayılar : ${tunedLinearModel.coefficients}")
    println(s"Sabit : ${tunedLinearModel.intercept}")

    println("En iyi lrmodelin parametreleri: ")
    println(tunedLinearModel.explainParams)


  }
}
