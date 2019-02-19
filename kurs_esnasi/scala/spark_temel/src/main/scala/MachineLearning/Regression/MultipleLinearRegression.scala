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
      .appName("MultipleLinearRegression")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    // Veri seti

    val df = spark.read.format("csv")
      .option("header", true)
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\Advertising.csv")

    println("OrijinalDF: ")
    df.show()

    //********* VERİ HAZIRLIĞI ************************

    val yeniSutunlar = Array("id", "TV","Radio","Newspaper","label")

    val df2 = df.toDF(yeniSutunlar:_*)

    println("İsimleri değişmiş DF")
    df2.show()

    var numerikDegiskenler = Array("TV","Radio")
    var label = Array("label")


    df2.describe().show()

    // varsayımlar karşılanıyor mu?

    // vector assembler
    import org.apache.spark.ml.feature.{VectorAssembler}
    val vectorAssembler = new VectorAssembler()
      .setInputCols(numerikDegiskenler)
      .setOutputCol("features")

    // Regresyon nesnesi
    import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
    val lrObj = new LinearRegression()
      .setLabelCol("label")
      .setFeaturesCol("features")

    // Pipeline
    import org.apache.spark.ml.{Pipeline}
    val pipelineObj = new Pipeline()
      .setStages(Array(vectorAssembler, lrObj))


    val Array(trainDF, testDF) = df2.randomSplit(Array(0.8, 0.2), 142L)

    val pipelineModel = pipelineObj.fit(trainDF)

    pipelineModel.transform(testDF).show()

    val lrModel = pipelineModel.stages(1).asInstanceOf[LinearRegressionModel]

    println("Sabit: " + lrModel.intercept)
    println("Katsayılar: " + lrModel.coefficients)
    println("rootMeanSquaredError: " + lrModel.summary.rootMeanSquaredError)
    println("r2: " + lrModel.summary.r2)
    println("r2adj: " + lrModel.summary.r2adj)
    println("pValues: " + lrModel.summary.pValues.mkString(","))
    println("tValues: " + lrModel.summary.tValues.mkString(","))

/*
Sabit: 2.8630452712927066
Katsayılar: [0.0441387582924728,0.19637772789978722,0.003943871526992222]
rootMeanSquaredError: 1.6561100287995882
r2: 0.8931175171003486
r2adj: 0.8910217821415319
pValues: 0.0,0.0,0.5717102604020492,3.810285420513537E-13
tValues: 27.723526858081737,20.32452728577851,0.5667562977703877,7.950153508044762
  y = 2.935593134859488 + (0.044210411496210966 * TV) + (0.19777489934012493 * Radio)

 */
// TV 50 radyo 10
    val predictDF = Seq((50.0, 10.0)).toDF("TV","Radio")
    predictDF.show()

    val predictVec = vectorAssembler.transform(predictDF)

    lrModel.transform(predictVec).show()
  }
}
