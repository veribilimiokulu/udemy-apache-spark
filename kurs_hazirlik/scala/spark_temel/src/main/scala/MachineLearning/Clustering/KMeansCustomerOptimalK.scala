package MachineLearning.Clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.clustering.{KMeans}
import org.apache.spark.ml.feature.{ StandardScaler, VectorAssembler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

object KMeansCustomerOptimalK extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)


  // SparkSession oluşturma
  val spark = SparkSession.builder()
    .appName("KMeansMallCustomerBasic").master("local[4]")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.codegen.wholeStage", "false")
    .getOrCreate()

  // SparkContext oluşturma (lazım olursa diye)
  val sc = spark.sparkContext
  import spark.implicits._

  // Veriyi Okuma ve dataframe oluşturma
  val df2 = spark.read.format("csv")
    .option("header","true")
    .option("sep",",")
    .option("inferSchema","true")
    .load("D:\\Datasets\\Mall_Customers.csv")

  ///////////////////////////////  VERİ KEŞFİ VE TEMİZLİĞİ  ////////////////////////////////////

  // İlk göz atma
  df2.show()

  // Sütun isimlerindeki boşluğu kaldırma
  val df = df2.withColumnRenamed("Annual Income (k$)","AnnualIncome")
    .withColumnRenamed("Spending Score (1-100)","SpendingScore")

  // Sütun isimleri sonrası kontrol
  df.show()

  // Nümerik niteliklerin temel istatistiksel bilgileri
  df.describe().show()

  // Null değer yok, Yaş içinde aykırı değer yok, Yıllık gelirde aykırı değer yok, harcama skoru belirlenen aralıkta (1-100)
  // Gende içinde Male,Female dışında başka bir değer yok

  // VectorAssembler
  val vectorAssembler = new VectorAssembler()  // nesne oluştur
    .setInputCols(Array("AnnualIncome","SpendingScore"))
    .setOutputCol("features")


  // StandartScaler : mesafe ölçümü olacak ve yaş, maaş gibi farklı ölçekte girdiler var
  val standartScaler = new StandardScaler() // nesne yarat
    .setInputCol("features") // girdi olarak vector assembler ın bir sütuna vektör olarak birleştirdiği niteliği al
    .setOutputCol("scaledFeatureVector") // çıktı ismini belirle
    .setWithStd(true) // hiperparametre
    .setWithMean(false) // hiperparametre


  def runKMeans(df:DataFrame, k:Int):PipelineModel = {
    // KMeans
    val kmeansObject = new KMeans() // nesne
      .setSeed(142) // random seed değeri başa bir değer de olabilir
      .setK(k) // kullanıcık değerini belirlemesi gerek. Bunu yapacağız.
      .setPredictionCol("cluster") // sonuçlar (küme numaralarının bulunacağı yeni sütun adı ne olsun)
      .setFeaturesCol("scaledFeatureVector") // girdi olarak StandartScaler çıktısını alıyoruz
      .setMaxIter(40) // Hiperparametre: iterasyon sayısı
      .setTol(1.0e-5) // Hiperparametre

    // pipeline model: parçaları birleştir
    val pipelineObject = new Pipeline()
      .setStages(Array(vectorAssembler, standartScaler, kmeansObject))

    // pipeline ile modeli eğitme ve pipeline model elde etme
    val pipelineModel = pipelineObject.fit(df)

    pipelineModel
  }


  //////////// optimal k için for döngüsü

  for (k <- 2 to 11){

    val pipelineModel = runKMeans(df, k)

    val transformedDF = pipelineModel.transform(df)


    // Kümeleme değerlendirici oluşturma
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaledFeatureVector")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")


    val score = evaluator.evaluate(transformedDF)

    println(k, score)

  }

  /// En iyi k değerine göre kümeleme yapma
    val pipelineModel2 = runKMeans(df, 5)
  val transformedDF2 = pipelineModel2.transform(df)

  transformedDF2.show()

}
