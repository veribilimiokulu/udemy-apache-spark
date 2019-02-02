package MachineLearning.Preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataCleaning {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("DataCleaning")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    /////////////////////// VERİ TEMİZLİĞİ İÇİN TAVSİYELER  ////////////////////////////////////////////////
    /*
      1. Tüm sütunları boşluk kontrolü yap.
      2. ? içeren workclass, occupation  var bunların ? içerdiği satırlar tekrar incelenmeli.
          ? işaretleri sistematik bir şekilde mi oluşmuş yoksa bu oluşum tesadüfi mi?
          ? kayıtlarının oluşması altında yatan bir mekanizma var mı?
          Bu sistematik hata yakalanırsa veri doldurma (imputation) yoksa satır silme yapılsın.
      3. workclass niteliğinde never-worked ve without-pay sınıfları ve
          occupation niteliğinde  Armed-Forces  sınıfı
        çok az tekrarlanmış. Veri setinden çıkarılabilir.
      4. education niteliğindeki:
              1st-4th, 5th-6th, 7th-8th: elementary-school
              9th, 10th, 11th, 12th: high-school
              Masters, Doctorate: high-education
              Bachelors, Some-college: undergraduate
         sınıfları yukarıdaki gibi birleştirilebilir.
      5. native_country'de ? var ve Hollanda 1 kez tekrarlanmış.
      6. output (hedef değişkendeki) >50K. ve <=50K. değerlerindeki "." kaldırılmalı
     */
    //////////////////////////////////  VERİ OKUMA /////////////////////////////////////
    // adult.data veri setini okuma
    val adultTrainDF = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("D:\\Datasets\\adult.data")

    // adult.test veri setini okuma
    val adultTestDF = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("D:\\Datasets\\adult.test")

    // test ve eğitim verisini birleştirme
    val adultWholeDF = adultTrainDF.union(adultTestDF)
    adultWholeDF.show(5)

    //====================================  VERİ TEMİZLİĞİ BAŞLIYOR ====================================================
    //==================================================================================================================
    /////////////////////////  1. Tüm sütunları boşluk kontrolü yap.  /////////////////////////////////////////////

    // Boşluk temizliği string değişkenleri için yapılır.
    val adultWholeDF1 = adultWholeDF
      .withColumn("workclass", trim(col("workclass")))
      .withColumn("education", trim(col("education")))
      .withColumn("marital_status", trim(col("marital_status")))
      .withColumn("occupation", trim(col("occupation")))
      .withColumn("relationship", trim(col("relationship")))
      .withColumn("race", trim(col("race")))
      .withColumn("sex", trim(col("sex")))
      .withColumn("native_country", trim(col("native_country")))
      .withColumn("output", trim(col("output")))




    ///////////////////// 2. OUTPUT İÇİNDEKİ "." TEMİZLİĞİ /////////////////////////////////////////////////////
    // Her ne kadar taslak planda output temizliğini son madde olarak yazmış olsak da. İşin kolay olması ve diğer temizlik
    // işlemlerini etkiliyor olması nedeniyle. Bu temizliği en başta yapalım.
    val adultWholeDF2 = adultWholeDF1
      .withColumn("output", regexp_replace(col("output"), "<=50K.", "<=50K"))
      .withColumn("output", regexp_replace(col("output"), ">50K.", ">50K"))


    // Temizlik sonucunu görelim.
    println("output groupby inceleme")
    adultWholeDF2.groupBy($"output")
      .agg(count($"*").as("sayi"))
      //.count()
      .show(false)
    // İstediğimiz gibi satır sayısı değişmemiş ancak sınıf sayısı 2'ye düştü.


    /// Null kontrolü
    var sayacForNull = 1
    for(sutun <- adultWholeDF2.columns){
      if(adultWholeDF2.filter(col(sutun).isNull).count() > 0){
        println(sayacForNull + ". " + sutun + " içinde NULL var")
      }else{
        println(sayacForNull+ ". " + sutun)
      }
      sayacForNull += 1
    }




    ///////////////////////////// 3.  "?" KONTROLLERİ  ///////////////////////////////////////////////////////////////
    println("\n ////////////// ? Kontrolü ///////////////////// \n")

/*
    2. ? içeren workclass, occupation  var bunların ? içerdiği satırlar tekrar incelenmeli.
      ? işaretleri sistematik bir şekilde mi oluşmuş yoksa bu oluşum tesadüfi mi?
    ? kayıtlarının oluşması altında yatan bir mekanizma var mı?
    Bu sistematik hata yakalanırsa veri doldurma (imputation) yoksa satır silme yapılsın.*/

    var sayacForQuestion = 1
    for(sutun <- adultWholeDF2.columns){
      if(adultWholeDF2.filter(col(sutun).contains("?")).count() > 0){
        println(sayacForQuestion + ". " + sutun + " içinde ? var")
      }else{
        println(sayacForQuestion+ ". " + sutun)
      }
      sayacForQuestion += 1
    }


    // Bunların hedef değişken output ile ilişkisini inceleyelim.
adultWholeDF2.select("workclass", "occupation", "native_country", "output")
      .filter(col("workclass").contains("?") || col("occupation").contains("?") ||
      col("native_country").contains("?") || col("output").contains("?"))
      .groupBy("workclass", "occupation", "native_country", "output").count()
      .orderBy(col("count").desc)
      .show(50)

    // Soru işaretlerinin dağılımı ve hedef değişken ile ilgisi tesadüfi görünüyor. Bu duru
      val adultWholeDF3 = adultWholeDF2
        .filter(!(col("workclass").contains("?") || col("occupation").contains("?") ||
          col("native_country").contains("?")))

    println("\n ? temizliği sonrası satır sayısı:: ", adultWholeDF2.count())
    println("\n ? temizliği sonrası satır sayısı:: ", adultWholeDF3.count())

    // ? işareti olan satırlar silindi.


    ///////////////////////////// 4.  ZAYIF SINIFLARI SİLME  ///////////////////////////////////////////////////////////////
    /*
    3. workclass niteliğinde never-worked ve without-pay sınıfları ve
          occupation niteliğinde  Armed-Forces  sınıfı
        çok az tekrarlanmış. Veri setinden çıkarılabilir.
     */

    val adultWholeDF4 = adultWholeDF3
      .filter(!(col("workclass").contains("never-worked") || col("workclass").contains("without-pay") ||
      col("occupation").contains("Armed-Forces") || col("native_country").contains("Holand-Netherlands")))

    println("\n ? temizliği sonrası satır sayısı:: ", adultWholeDF3.count())
    println("\n ? temizliği sonrası satır sayısı:: ", adultWholeDF4.count())


    ///////////////////////////// 5.  EĞİTİM KATEGORİ BİRLEŞTİRME ///////////////////////////////////////////////////////////////
/*
    4. education niteliğindeki:
      1st-4th, 5th-6th, 7th-8th: elementary-school
    9th, 10th, 11th, 12th: high-school
    Masters, Doctorate: high-education
    Bachelors, Some-college: undergraduate
    sınıfları yukarıdaki gibi birleştirilebilir.
*/

    val adultWholeDF5 = adultWholeDF4
      .withColumn("education_merged",
        when(col("education").isin("1st-4th", "5th-6th", "7th-8th"), "Elementary-School")
          .when(col("education").isin("9th", "10th", "11th", "12th"), "High-School")
          .when(col("education").isin("Masters", "Doctorate"), "Postgraduate")
          .when(col("education").isin("Bachelors", "Some-college"), "Undergraduate")
          .otherwise(col("education")))


    // Birleştirme sonucunu görelim.
    println("education birleştirme sonucunu")
    adultWholeDF5.groupBy($"education_merged")
      .agg(count($"*").as("sayi"))
      //.count()
      .show(false)

    // Evet yeni bir sütun ekledik. education_merged. Daha sade.

    adultWholeDF5.show(5)

    ///////////////////////////// 9. TEMİZ VERİYİ DİSKE YAZALIM /////////////////////////////////
    // Sütun sırasını değiştirelim. education_merge'i education yanına alalım
    val nitelikSiralama = Array[String]("workclass", "education", "education_merged", "marital_status", "occupation", "relationship", "race",
      "sex", "native_country", "age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week","output")

    val adultWholeDF6 = adultWholeDF5.select(nitelikSiralama.head, nitelikSiralama.tail:_*)
    adultWholeDF6.show(5)

// Diske yazma

    adultWholeDF6
      .coalesce(1)
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("D:\\\\Datasets\\\\adult_preprocessed")
    // Yazdığımız yerde dosyayı Notepad++ ile kontrol edelim
    // Nitelik sırası düzgün ve satır sayısı da 45.208 (başlık dahil) sorun yoktur.





  }
}
