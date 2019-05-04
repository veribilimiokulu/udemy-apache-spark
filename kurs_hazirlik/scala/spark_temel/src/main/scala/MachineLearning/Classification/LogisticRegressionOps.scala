package MachineLearning.Classification

import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.{SparkSession, DataFrame, Dataset}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{functions => F}
import org.apache.spark.ml.{PipelineModel, Pipeline, PipelineStage}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoderEstimator, VectorAssembler}
object LogisticRegressionOps {
  def main(args: Array[String]): Unit = {

    // Log seviyesini ayarlama
   // Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession
    val spark = SparkSession.builder()
      .appName("Logistic Regression Classification")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executormemory","4g")
      .getOrCreate()

    // Veri okuma
    val df = spark.read.format("csv")
      .option("header","true")
      .option("inferSchema","true")
      .option("sep",",")
      .load("D:\\Datasets\\adult_preprocessed.csv")


    //////////////////// 1. Veri Keşfi ////////////////////////////////////////////////////////

    // 1.1. Veri setine göz atma
    df.show()


    //  1.2. Veri seti şemasını görme
    df.printSchema()


    // 1.3. Nitelikleri Nümerik ve Kategorik olarak ayırmak
    def identifyCategoricAndNumeric(df: DataFrame): (Array[String], Array[String]) = {
        /*
        Bu fonksiyon parametre olarak analiz edilecek dataframe'i alır. String olan nitelikleri kategorik olmayanları da
        nümerik olarak ayırarak ve iki farklı Array[String] döner.
         */
      var categoricCols = Array[String]()
      var numericCols = Array[String]()
      for (col <- df.columns){
        if(df.schema(col).dataType  == StringType){
          categoricCols = categoricCols :+ col
        }else{
          numericCols = numericCols :+ col
        }
      }

      (categoricCols, numericCols)
    }

    // Fonksiyonu kullanarak kategorik ve nümerik nitelikleri elde etme
    var (kategorik, numerik) = identifyCategoricAndNumeric(df)

    // kategorik ve nimerik nitelikleriyazdırma
    println("\nKategorik Nitelikler:")
    kategorik.foreach(println(_))
    println("\nNümerik Nitelikler:")
    numerik.foreach(println(_))


    // 1.4. Nümerik Nitelikler ile Betimsel İstatistik
    df.describe(numerik:_*).show()

    // 1.5. Kategorik niteliklerin kategorilerini ve frekans dağılımlarını groupBy kullanarak incelemek
    for (catCol <- kategorik){
      println(s"\n$catCol için groupby")
      df.groupBy(catCol).count().sort(F.desc("count")).show()
    }

  //////////////////////////////////////  2. Veri Temizliği ve Ön İşleme //////////////////////////////////////
    // 2.1. Null kontrolü
    println("\nNull kontrolü: ")
    for(col <- df.columns){
      if(df.filter(df.col(col).isNull).count() > 0){
        println(s"$col içinde null VAR")
      }else{
        println(s"$col içinde null YOK")
      }
    }
    // daha önceden temizlediğimiz bir veri olduğu için null değer yok


    //
    // StringIndexer, OneHotEncoder ve VectorAssembler için kategorik ön hazırlık
    // Kategorik değişkenler için StringIndexer ve OneHotEncoderları toplayacak boş Array
    var categoricalColsPipeStages = Array[PipelineStage]()

    // Kategorik değişkenler için OneHotEncoder çıktı isimlerini toplayıp VectorAssembler'a verecek boş Array
    var catsForVectorAssembler = Array[String]()

    // StringIndexer, OneHotEncder ve VectorAssembler için for loop yap. Tüm kategorik değişkenleri
    // sırasıyla StringIndexer, OneHotEncoder'dan geçir ve VectorAssembler'a hazır hale getir.
    // Yani Vector Assembler'a hangi sütun isimlerini verecekisek onları bir Array'de topla
    for (col <- kategorik) {

      val stringIndexer = new StringIndexer()
        .setHandleInvalid("skip")
        .setInputCol(col)
        .setOutputCol(s"${col}Index")
      val encoder = new OneHotEncoderEstimator().setInputCols(Array(s"${col}Index"))
        .setOutputCols(Array(s"${col}OneHot"))

      categoricalColsPipeStages = categoricalColsPipeStages :+ stringIndexer :+ encoder
      catsForVectorAssembler = catsForVectorAssembler :+ s"${col}OneHot"
    }






  }
}
