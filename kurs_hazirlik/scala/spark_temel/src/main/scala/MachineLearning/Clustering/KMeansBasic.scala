package MachineLearning.Clustering
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions => F}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{OneHotEncoderEstimator, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object KMeansBasic {
  def main(args: Array[String]): Unit = {
    // Log seviyesini ayarlama
    Logger.getLogger("org").setLevel(Level.ERROR)


    // SparkSession oluşturma
    val spark = SparkSession.builder()
      .appName("KMeans Clustering").master("local[4]")
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
      .setStages(categoricalColsPipeStages ++ Array(vectorAssembler, standartScaler, kmeansObject))

    // pipeline ile modeli eğitme ve pipeline model elde etme
    val pipelineModel = pipelineObject.fit(df)

    // Veri setini eğitilmiş modelden geçirelim ve tahminleri görelim: Kim hangi kümeye düşmüş
    val transformedDF = pipelineModel.transform(df)

    transformedDF.show()
    /*
    +----------+------+---+------------+-------------+-----------+-------------+--------------------+--------------------+-------+
    |CustomerID|Gender|Age|AnnualIncome|SpendingScore|GenderIndex| GenderOneHot|            features| scaledFeatureVector|cluster|
    +----------+------+---+------------+-------------+-----------+-------------+--------------------+--------------------+-------+
    |         1|  Male| 19|          15|           39|        1.0|    (1,[],[])|[19.0,15.0,39.0,0.0]|[1.36015391423519...|      2|
    |         2|  Male| 21|          15|           81|        1.0|    (1,[],[])|[21.0,15.0,81.0,0.0]|[1.50332801047048...|      2|
    |         3|Female| 20|          16|            6|        0.0|(1,[0],[1.0])| [20.0,16.0,6.0,1.0]|[1.43174096235284...|      4|
    |         4|Female| 23|          16|           77|        0.0|(1,[0],[1.0])|[23.0,16.0,77.0,1.0]|[1.64650210670576...|      4|
    |         5|Female| 31|          17|           40|        0.0|(1,[0],[1.0])|[31.0,17.0,40.0,1.0]|[2.21919849164690...|      4|
     */

    // Pekala, ideal k sayısını neye göre belirleyeceğiz. Cevabı ClusteringEvaluation dersinde.



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

  //
}
