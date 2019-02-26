package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.sql.SparkSession

object RegressionTuning {
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



    ///////////////////////////////// MODEL TUNING /////////////////////////////////////////////////
    //================================================================================================

    // Kullanılacak parametreler
   import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
    val paramGrid = new ParamGridBuilder()
      .addGrid(linearRegressionObject.aggregationDepth, Array(2,5))
      .addGrid(linearRegressionObject.elasticNetParam, Array(0.0, 0.2))
      .addGrid(linearRegressionObject.epsilon, Array(1.35, 1.50))
      .addGrid(linearRegressionObject.maxIter, Array(20))
      .addGrid(linearRegressionObject.regParam, Array(0.0, 0.01))
      .addGrid(linearRegressionObject.solver, Array("auto", "l-bfgs"))
      .addGrid(linearRegressionObject.tol, Array(1.0E-6, 1.0E-4))
      .build()

    // Croos Validation
    import org.apache.spark.ml.evaluation.RegressionEvaluator
    val cv = new CrossValidator()
      .setEstimator(pipelineObject)
      .setEvaluator(new RegressionEvaluator())
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(2)

    //Model eğitimi
   val cvModel = cv.fit(trainDF)

  val bestPipelineModel = cvModel.bestModel.asInstanceOf[PipelineModel]

    val tunedLinearModel = bestPipelineModel.stages.last.asInstanceOf[LinearRegressionModel]

    // Regresyon modele ait  istatistikler
    println(s"RMSE: ${tunedLinearModel.summary.rootMeanSquaredError}")
    println(s"R kare : ${tunedLinearModel.summary.r2}")
    println(s"Düzeltilmiş R kare : ${tunedLinearModel.summary.r2adj}")
    // Değişken katsayılarını görme. Son değer sabit
    println(s"Katsayılar : ${tunedLinearModel.coefficients}")
    println(s"Sabit : ${tunedLinearModel.intercept}")

    println("En iyi lrmodelin parametreleri: ")
    println(tunedLinearModel.explainParams)

      /*
      RMSE: 3.6000604604965685
      R kare : 0.8334028383100218
      Düzeltilmiş R kare : 0.8321213216816374
      Katsayılar : [-0.10207977217556083,-0.01730405510395077,0.09664562246291719,0.03203142512499621,-0.07372197815617171,0.11463807666023659,-0.4476273332867619,10.034142933349152,0.9522878985420877,-1.0885495625305033]
      Sabit : 259.29826263941743
      p değerler: [5.60965433649141E-5,0.0,0.0,3.6021994853641104E-7,0.0,0.011241136416126674,0.0,0.0,0.0,0.0013802080023210817,3.3990309589171375E-7]
      t değerler: [-4.0420445369064755,-16.28718060045066,8.616328568449806,5.115301615402378,-8.845366289206154,2.5387376545619507,-23.027552421397825,11.327224910203768,15.140137022059411,-3.2056813417655254,5.126469305045018]
      lrModel parametreleri:
      aggregationDepth: suggested depth for treeAggregate (>= 2) (default: 2)
      elasticNetParam: the ElasticNet mixing parameter, in range [0, 1]. For alpha = 0, the penalty is an L2 penalty. For alpha = 1, it is an L1 penalty (default: 0.0)
      epsilon: The shape parameter to control the amount of robustness. Must be > 1.0. (default: 1.35)
      featuresCol: features column name (default: features, current: features)
      fitIntercept: whether to fit an intercept term (default: true)
      labelCol: label column name (default: label, current: label)
      loss: The loss function to be optimized. Supported options: squaredError, huber. (Default squaredError) (default: squaredError)
      maxIter: maximum number of iterations (>= 0) (default: 100)
      predictionCol: prediction column name (default: prediction)
      regParam: regularization parameter (>= 0) (default: 0.0)
      solver: The solver algorithm for optimization. Supported options: auto, normal, l-bfgs. (Default auto) (default: auto)
      standardization: whether to standardize the training features before fitting the model (default: true)
      tol: the convergence tolerance for iterative algorithms (>= 0) (default: 1.0E-6)
      weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (undefined)
      (0.0,AdultMortality)
       */





  }
}
