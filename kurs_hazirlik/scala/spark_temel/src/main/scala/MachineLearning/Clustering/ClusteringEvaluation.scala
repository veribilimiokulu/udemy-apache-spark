package MachineLearning.Clustering
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object ClusteringEvaluation {
  def main(args: Array[String]): Unit = {
    // Log seviyesini ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)


    // SparkSession oluşturma
    val spark = SparkSession.builder()
      .appName("KMeans Clustering").master("local[6]")
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

    // Cinsiyete göre toplam ve ortalama gelir ve harcama skorları
    df.groupBy("Gender").agg(
      F.count("CustomerID").as("TotalCount"),
      F.sum("AnnualIncome").as("TotalIncome"),
      F.mean("AnnualIncome").as("AvgIncome"),
      F.mean("SpendingScore").as("AvgSpendingScore")

    ).show()
    /*
    +------+----------+-----------+-----------------+------------------+
    |Gender|TotalCount|TotalIncome|        AvgIncome|  AvgSpendingScore|
    +------+----------+-----------+-----------------+------------------+
    |Female|       112|       6636|            59.25|51.526785714285715|
    |  Male|        88|       5476|62.22727272727273| 48.51136363636363|
    +------+----------+-----------+-----------------+------------------+

    Erkekler daha çok kazanırken kadınlar daha çok harcama yapmış
     */

    ///////////////////////////////  VERİ HAZIRLIĞI  ////////////////////////////////////
    // Hangi nitelikleri analize sokalım?
    // Önce girmeyecekleri tespit edelim: CustomerID bize bir bilgi sağlamaz onu hariç bırakalım.
    // Diğer nitelikler kümelemeye etkili olabilir.
    val notToAnalyzed = Array("CustomerID")

    // Analize girmeyecek nitelikleri filtreleme ve sadece gireceklerden bir Array oluşturma
    val colsToAnalyze = df.columns.toSet.diff(notToAnalyzed.toSet).toArray.sorted

    // kategorik değişkenlerle nümerikleri ayıran fonksiyonu kullanarak ayrı ayrı iki Array'e atama
    var (categoricalCols, numericalCols) = identifyCategoricAndNumeric(df,colsToAnalyze)

    // Ayırma sonucunu görelim
    println("Kategorik nitelikler:")
    categoricalCols.foreach(println(_))
    println("\nNümerik nitelikler:")
    numericalCols.foreach(println(_))
    /*
    Çıktı:
    Kategorik nitelikler:
    Gender

    Nümerik nitelikler:
    Age
    AnnualIncome
    SpendingScore

    Sonuç harika
     */


    // Veri hazırlığı

    // StringIndexer, OneHotEncoder ve VectorAssembler için kategorik ön hazırlık
    // Kategorik değişkenler için StringIndexer ve OneHotEncoderları toplayacak boş Array
    var categoricalColsPipeStages = Array[PipelineStage]()

    // Kategorik değişkenler için OneHotEncoder çıktı isimlerini toplayıp VectorAssembler'a verecek boş Array
    var catsForVectorAssembler = Array[String]()

    // StringIndexer, OneHotEncder ve VectorAssembler için for loop yap. Tüm kategorik değişkenleri
    // sırasıyla StringIndexer, OneHotEncoder'dan geçir ve VectorAssembler'a hazır hale getir.
    // Yani Vector Assembler'a hangi sütun isimlerini verecek isek onları bir Array'de topla
    for (col <- categoricalCols) {

      val stringIndexer = new StringIndexer()  // StringIndexer nesnesi oluştur
        .setHandleInvalid("skip")  // kayıp kategorileri ihmal et
        .setInputCol(col) // giri sütun ne
        .setOutputCol(s"${col}Index")  // çıktı sütün ismi sütun ismi Index eklenerek oluşsun


      val encoder = new OneHotEncoderEstimator() // nesne oluştur
        .setInputCols(Array(s"${col}Index"))  // stringIndexer çıktı sütunu burada girdi oluyor
        .setOutputCols(Array(s"${col}OneHot")) // çıktı sütun ismi OneHot eklenerek oluşsun

      // Her döngüde pipeline stage olarak string indexer ve encoder ekle
      categoricalColsPipeStages = categoricalColsPipeStages :+ stringIndexer :+ encoder

      // Vector Assmebler da kullanılmak üzere her döngüde OneHotEncoder çıktısı sütun isimlerini listeye ekle
      catsForVectorAssembler = catsForVectorAssembler :+ s"${col}OneHot"
    }

    // VectorAssembler
    val vectorAssembler = new VectorAssembler()  // nesne oluştur
      .setInputCols(numericalCols ++ catsForVectorAssembler) // nümerik nitelikler ile içinde onehotencoder çıktı nitelikleri olan kategorik nitelikler
      .setOutputCol("features")  // çıktı sütun ismi features olsun


    // StandartScaler : mesafe ölçümü olacak ve yaş, maaş gibi farklı ölçekte girdiler var
    val standartScaler = new StandardScaler() // nesne yarat
      .setInputCol("features") // girdi olarak vector assembler ın bir sütuna vektör olarak birleştirdiği niteliği al
      .setOutputCol("scaledFeatureVector") // çıktı ismini belirle
      .setWithStd(true) // hiperparametre
      .setWithMean(false) // hiperparametre

   def runKMeans(k:Int, df:DataFrame) :PipelineModel= {

      // KMeans
      val kmeansObject = new KMeans()  // nesne
        .setSeed(142) // random seed değeri başa bir değer de olabilir
        .setK(k) // kullanıcık değerini belirlemesi gerek. Bunu yapacağız.
        .setPredictionCol("cluster") // sonuçlar (küme numaralarının bulunacağı yeni sütun adı ne olsun)
        .setFeaturesCol("scaledFeatureVector") // girdi olarak StandartScaler çıktısını alıyoruz
        .setMaxIter(40) // Hiperparametre: iterasyon sayısı
        .setTol(1.0e-5) // Hiperparametre

      // pipeline model: parçaları birleştir
      val pipelineObject = new Pipeline()
        .setStages(categoricalColsPipeStages ++ Array(vectorAssembler, standartScaler, kmeansObject))

      // pipeline ile modeli eğitme ve pipeline model elde etme
      val pipelineModel = pipelineObject.fit(df)

      pipelineModel
   }
    // Kümeleme değerlendirici oluşturma
    val evaluator = new ClusteringEvaluator()
      .setFeaturesCol("scaledFeatureVector")
      .setPredictionCol("cluster")
      .setMetricName("silhouette")

  for(k <- 2 until 15){
    val pipelineModel = runKMeans(k,df)
    val transformedDF = pipelineModel.transform(df)



    val score = evaluator.evaluate(transformedDF)

    println(k, score)

  }
    /*
      Sonuç:
      (2,0.41960650030709895)
      (3,0.3916638445952382)
      (4,0.4422938108326868)
      (5,0.4408184025788379)
      (6,0.4593208608990903)
      (7,0.5060552346658673)
      (8,0.5301283810738117)
      (9,0.5819959926685684)
      (10,0.48487448850484816)
      (11,0.5431033825916344)
      (12,0.5327438506690634)
      (13,0.5373390223869112)
      (14,0.5750759917116559)

       En yüksek değer k'nın 9 olduğu 0.58 değeri
       optimal küme sayısı olarak 9 alınabilir ancak değerler arası
       birbirine çok yakın.
       Farklı niteliklerle (örneğin yaş ve cinsiyet çıkarılarak) k'nın en düşük ve skorun da anlamlı derecede farklı (büyük)
       olduğu bir nokta aranabilir.
     */


    // Optimal k değeri ile kümeleme
    val optimalPipelineModel = runKMeans(k=9,df)

    // modelden dataframe'i geçirip sonuçları görme (kim hangi kümede)
    val sonucDF = optimalPipelineModel.transform(df)

    sonucDF.show()

    // Hangi kümeye ka kişi düşmüş
    sonucDF.groupBy("cluster").agg(
      F.count("*").as("counts")
    ).show()










  } // ana fonksiyon sonu
  ////////////////////  FONKSİYONLAR  /////////////////////////////
  // Kategorik ve nümerik nitelikleri birbirinden ayırma
  def identifyCategoricAndNumeric(df:DataFrame,colsToAnalyze: Array[String]): (Array[String], Array[String]) = {
    /*
    Bu fonksiyon parametre olarak dataframe(df) ve analize girecek sütun isimlerini (colsToAnalyze) alır ve sonuç olarak
    kategorik ve nümerik niteliklerin bulunduğu 2 array döndürür.
     */
    var categoricCols = Array[String]()
    var numericCols = Array[String]()
    for (col <- colsToAnalyze) {
      if (df.schema(col).dataType == StringType) {
        categoricCols = categoricCols ++ Array(col)
      } else {
        numericCols = numericCols ++ Array(col)
      }
    }

    (categoricCols, numericCols)
  }

}
