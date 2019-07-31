package MachineLearning.Clustering


import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.{Pipeline}
import org.apache.spark.ml.clustering.{KMeans}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.{SparkSession}

object KMeansMallCustomerBasic extends App {
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


  // KMeans
  val kmeansObject = new KMeans()  // nesne
    .setSeed(142) // random seed değeri başa bir değer de olabilir
    .setK(5) // kullanıcık değerini belirlemesi gerek. Bunu yapacağız.
    .setPredictionCol("cluster") // sonuçlar (küme numaralarının bulunacağı yeni sütun adı ne olsun)
    .setFeaturesCol("scaledFeatureVector") // girdi olarak StandartScaler çıktısını alıyoruz
    .setMaxIter(40) // Hiperparametre: iterasyon sayısı
    .setTol(1.0e-5) // Hiperparametre

  // pipeline model: parçaları birleştir
  val pipelineObject = new Pipeline()
    .setStages( Array(vectorAssembler, standartScaler, kmeansObject))

  // pipeline ile modeli eğitme ve pipeline model elde etme
  val pipelineModel = pipelineObject.fit(df)

  val transformedDF = pipelineModel.transform(df)

  transformedDF.show()

  /*
+----------+------+---+------------+-------------+--------------+--------------------+-------+
|CustomerID|Gender|Age|AnnualIncome|SpendingScore|      features| scaledFeatureVector|cluster|
+----------+------+---+------------+-------------+--------------+--------------------+-------+
|         1|  Male| 19|       15000|           39|[15000.0,39.0]|[0.57110829030364...|      1|
|         2|  Male| 21|       15000|           81|[15000.0,81.0]|[0.57110829030364...|      2|
|         3|Female| 20|       16000|            6| [16000.0,6.0]|[0.60918217632388...|      1|
+----------+------+---+------------+-------------+--------------+--------------------+-------+
   */















}
