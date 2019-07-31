package MachineLearning.Clustering


import MachineLearning.Clustering.KMeansMallCustomerBasic.df
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KMeansMallCustomerBasic extends App{

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Spark session
  val spark = SparkSession.builder()
    .appName("KMeansMallCustomerBasic")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._

  // veri okuma
  val df = spark.read.format("csv")
    .option("header",true)
    .option("sep",",")
    .option("inferSchema",true)
    .load("D:/Datasets/Mall_Customers.csv")

 // df.show()

  df.printSchema()

  df.describe().show()


  val vectorAssembler = new VectorAssembler()
    .setInputCols(Array("AnnualIncome", "SpendingScore"))
    .setOutputCol("features")


  val standardScaler = new StandardScaler()
    .setInputCol("features")
    .setOutputCol("scaledFeatures")

def runKMeans(df:DataFrame, k:Int):PipelineModel= {
  val kMeansObject = new KMeans()
    .setFeaturesCol("scaledFeatures")
    .setSeed(142)
    .setPredictionCol("cluster")
    .setK(k)
  val pipelineObj = new Pipeline()
    .setStages(Array(vectorAssembler, standardScaler, kMeansObject))

  val pipelineModel = pipelineObj.fit(df)

  pipelineModel
}







 //val transformedDF = pipelineModel.transform(df)

 // transformedDF.show(false)

  //transformedDF.groupBy("cluster").count().show()

/*
  for (k <- 2 to 11){

    val pipelineModel = runKMeans(df,k)

    val transformedDF = pipelineModel.transform(df)

    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaledFeatures")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")


    val score = evaluator.evaluate(transformedDF)

    println(k, score)

  }
  */

  val pipelineModel = runKMeans(df, 5)
  val transformedDF = pipelineModel.transform(df)

  transformedDF.show()







}
