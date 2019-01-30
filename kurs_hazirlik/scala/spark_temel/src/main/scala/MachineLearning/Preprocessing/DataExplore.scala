package MachineLearning.Preprocessing

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object DataExplore {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("DataExplore")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._


    //********* VERİ SETİNİ OKUMA ÖNCESİ KONTROLLER ************************
  // 1. Veri setine ulaşma
  //    adult.data ve adult.test adında iki veri dosyası var
  // 2. Mümkünse notepad++ içine bakalım. Baktık. virgülle ayrılmış ve başlıkları olan bir dosya


    //********* VERİ SETİNİ OKUMA  ************************
    // adult.data veri setini okuma
    val adultTrainDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\adult.data")


    // adult.test veri setini okuma
    val adultTestDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\adult.test")


    // okunan dataframe'e ilk bakış
    println("\n adultTrainDF")
    //adultTrainDF.show(5)

    // okunan dataframe'e ilk bakış
    println("\n adultTestDF")
    //adultTestDF.show(5)

    // İki veri setini birleştirelim
    val adultWholeDF = adultTrainDF.union(adultTestDF)

    adultWholeDF.show(5)


    // Acaba alttaki DF'in başlıkları ne oldu?
    adultWholeDF.select("workclass").filter($"workclass".contains("workclass")).show()

    // Emin olamadıysak bir de groupBy ile bakalım
    println("workclass groupby inceleme")
    adultWholeDF.groupBy($"workclass")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show()

    // İkinci veri setinin başlıkları veri setine alınmamış görünüyor.
    // Elimizde şuan üç DF var
    println("adultTrainDF satır sayısı: ")
    println(adultTrainDF.count())
    println("adultTestDF satır sayısı: ")
    println(adultTestDF.count())
    println("adultWholeDF satır sayısı: ")
    println(adultWholeDF.count())

    //********* VERİ SETİNİ İNCELEME ŞEMA İLE KARŞILAŞTIRMA  ************************
    // adultWholeDF şeması: Spark'ın çıkarımda bulunduğu şema ile DF'i kontrol edelim.
    println("adultWholeDF şeması: ")
    adultWholeDF.printSchema()



    //////////////////////  NÜMERİK DEĞİŞKENLERİN İSTATİSTİKLERİ ////////////////////////////////////////////
    // Nümerik değişkenlerin istatistiklerini görelim
    println("Nümerik değişkenlerin istatistiklerini görelim")
    adultWholeDF.describe("fnlwgt","education_num","capital_gain","capital_loss","hours_per_week").show()



    /////////////////////// KATEGORİK DEĞİŞKENLERİN İNCELENMESİ  ////////////////////////////////////////////////
    // Kategorik değişkenlerin incelenmesinde groupBy() kullanmak daha çok bilgi verir.

    // 1. =====================  workclass  =================================

    println("workclass groupby inceleme")
    adultWholeDF.groupBy($"workclass")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show()

      /*
      workclass groupby inceleme
          +-----------------+----------+
          |        workclass|sayi       |
          +-----------------+----------+
          |        State-gov|      1981|
          |      Federal-gov|      1432|
          | Self-emp-not-inc|      3862|
          |        Local-gov|      3136|
          |          Private|     33906|
          |                ?|      2799|
          |     Self-emp-inc|      1695|
          |      Without-pay|        21|
          |     Never-worked|        10|
          +-----------------+----------+
        * Yorum: 2.799 adet ? var. Bu nedir. Muhtelemen kayıp bilgi.
        * Daha sonra never-worked ve without-pay sınıfları çok az tekrarlanmış. Bunların da veri setinden
        * çıkarılması düşünebilir.
        *
        * */


    // 2. =====================  education  =================================

      println("education groupby inceleme")
      adultWholeDF.groupBy($"education")
        .agg(F.count($"*").as("sayi"))
      //.count()
      .show()
    /*
          * education groupby inceleme
          +-------------+----------+
          |    education|sayi       |
          +-------------+----------+
          |  Prof-school|       834|
          |         10th|      1389|
          |      7th-8th|       955|
          |      5th-6th|       509|
          |   Assoc-acdm|      1601|
          |    Assoc-voc|      2061|
          |      Masters|      2657|
          |         12th|       657|
          |    Preschool|        83|
          |          9th|       756|
          |    Bachelors|      8025|
          |    Doctorate|       594|
          |      HS-grad|     15784|
          |         11th|      1812|
          | Some-college|     10878|
          |      1st-4th|       247|
          +-------------+----------+
          Yorum: Genel bir sıkıntı görünmüyor ancak çok fazla kategori var belki bazıları birleştirilebilir.

          1st-4th, 5th-6th, 7th-8th: elementary-school
          9th, 10th, 11th, 12th: high-school
          Masters, Doctorate: high-education
          Bachelors, Some-college: undergraduate
          */


    // 3. =====================  marital_status  =================================

    println("marital_status groupby inceleme")
    adultWholeDF.groupBy($"marital_status")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)

    /*
    * marital_status groupby inceleme
          +--------------------+----------+
          |      marital_status|sayi      |
          +--------------------+----------+
          |             Widowed|      1518|
          | Married-spouse-a...|       628|
          |   Married-AF-spouse|        37|
          |  Married-civ-spouse|     22379|
          |            Divorced|      6633|
          |       Never-married|     16117|
          |           Separated|      1530|
          +--------------------+----------+
      Yorum: Sorun görünmüyor.
      */


    // 4. =====================  occupation  =================================

    println("occupation groupby inceleme")
    adultWholeDF.groupBy($"occupation")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)
          /*
          * occupation groupby inceleme
          +------------------+--------+
          |occupation        |sayi    |
          +------------------+--------+
          | Farming-fishing  |1490    |
          | Handlers-cleaners|2072    |
          | Prof-specialty   |6172    |
          | Adm-clerical     |5611    |
          | Exec-managerial  |6086    |
          | Craft-repair     |6112    |
          | Sales            |5504    |
          | ?                |2809    |
          | Tech-support     |1446    |
          | Transport-moving |2355    |
          | Protective-serv  |983     |
          | Armed-Forces     |15      |
          | Machine-op-inspct|3022    |
          | Other-service    |4923    |
          | Priv-house-serv  |242     |
          +------------------+--------+

          Yorum: 2809 tane ? var. Bunlar muhtemelen bilinmeyenler. Ayrıca Armed-Forces 15 kişi.
          Bu sınıfa ait kayıtlar çıkarılabilir.
          * */
    // 5. =====================  relationship  =================================

    println("relationship groupby inceleme")
    adultWholeDF.groupBy($"relationship")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)
        /*
        * relationship groupby inceleme
        +---------------+-----+
        |relationship   |sayi |
        +---------------+-----+
        | Husband       |19716|
        | Own-child     |7581 |
        | Not-in-family |12583|
        | Other-relative|1506 |
        | Wife          |2331 |
        | Unmarried     |5125 |
        +---------------+-----+
        * Yorum: Sorun görünmüyor. Ailede yaşamayanların fazlalığı dikkat çekici.
        *
        * */

    // 6. =====================  race  =================================

    println("race groupby inceleme")
    adultWholeDF.groupBy($"race")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)

    /*
    * race groupby inceleme
      +-------------------+-----+
      |race               |sayi |
      +-------------------+-----+
      | Asian-Pac-Islander|1519 |
      | Black             |4685 |
      | Other             |406  |
      | White             |41762|
      | Amer-Indian-Eskimo|470  |
      +-------------------+-----+
    *
    * Yorum: Sorun görünmüyor. Çoğunluk white.
    * */

    // 7. =====================  sex  =================================

    println("sex groupby inceleme")
    adultWholeDF.groupBy($"sex")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)

    // Yorum: Üçte biri kadın kalanı erkek

    // 8. =====================  native_country  =================================

    println("native_country groupby inceleme")
    adultWholeDF.groupBy($"native_country")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)

    // Yorum: Büyük çoğunluk USA'den


    // 9. =====================  output  =================================

    println("output groupby inceleme")
    adultWholeDF.groupBy($"output")
      .agg(F.count($"*").as("sayi"))
      //.count()
      .show(false)

    // Yorum: "." içeren sonuçlar var. Bunların temizlenmesi gerekir.


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
      5. output (hedef değişkendeki) >50K. ve <=50K. değerlerindeki "." kaldırılmalı

     */

  }
}
