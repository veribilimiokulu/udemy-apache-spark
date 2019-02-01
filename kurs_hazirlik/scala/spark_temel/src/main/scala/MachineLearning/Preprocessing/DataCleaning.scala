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
      .appName("DataExplore")
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

    // Kategorik ve nümerik nitelikleri farklı değişkenlerde tutalım.
        val kategorikNitelikler = Seq("workclass", "education", "marital_status", "occupation", "relationship", "race",
          "sex", "native_country", "output")
        val numerikNitelikler = Seq("age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week")

    // Boşluk temizliğini sadece string niteliklere yapabiliriz.
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
      // . içerenleri filtreleyelim ancak "!" ile tersini yapalım.
      val adultWholeDF2 = adultWholeDF1.filter(!(col("output").contains(".")))

      // Temizlik sonucunu görelim.
      println("output groupby inceleme")
      adultWholeDF2.groupBy($"output")
        .agg(count($"*").as("sayi"))
        //.count()
        .show(false)

      // İstediğimiz gibi satır sayısı değişmemiş ancak sınıf sayısı 2'ye düştü.





      ////////////////////////// 3. NULL KONTROLÜ //////////////////////////////////////////////////////////////
      val sutunlar = adultWholeDF2.columns
      //sutunlar.foreach(println(_))

      // Her bir sütun için for dongüsü ile o sütunda filter() metodu ile null kontrolü yapalım ve bunları sayalım. Eğer
      // O sütunda en az bir adet null varsa yani >0 ise o sütunda null olduğunu ekrana yazdıralım.
      println("\n ////////////// Null Kontrolü ///////////////////// \n")
      var sayacForNull = 1
      for (sutun <- sutunlar) {
        if (adultWholeDF2.filter(col(sutun).isNull).count() > 0) {
          println(sayacForNull + ". " + sutun + " içinde null değer var.")
        } else {
          println(sayacForNull + ". " + sutun)
        }
        sayacForNull += 1
      }
      // adultWholeDF'de herhangi bir null değeri yok.





      ///////////////////////////// 4.  "?" KONTROLLERİ  ///////////////////////////////////////////////////////////////
      println("\n ////////////// ? Kontrolü ///////////////////// \n")
      var sayacForQuestion = 1
      for (sutun <- sutunlar) {
        if (adultWholeDF2.filter(col(sutun).contains("?")).count() > 0) {
          println(sayacForNull + ". " + sutun + " içinde ? var.")
        } else {
          println(sayacForNull + ". " + sutun)
        }
        sayacForQuestion += 1
      }
      // workclass, occupation, native_country içinde ? var.


      // Bunların hedef değişken output ile ilişkisini inceleyelim.
      adultWholeDF2.select("workclass", "occupation", "native_country", "output")
        .filter(col("workclass").contains("?") || col("occupation").contains("?") || col("native_country").contains("?"))
        .groupBy("workclass", "occupation", "native_country", "output").count()
        .orderBy(col("count").desc)
        .show(50)

      // Soru işaretlerinin dağılımı ve hedef değişken ile ilgisi tesadüfi görünüyor. Bu durumda ? işareti içeren satırları veri setinden çıkaralım.
      val adultWholeDF3 = adultWholeDF2
        .filter(!(col("workclass").contains("?") || col("occupation").contains("?") || col("native_country").contains("?")))
      println("\n ? temizliği sonrası satır sayısı:: ", adultWholeDF3.count())

      // ? işareti olan satırlar silindi.





      ////////////////////////////////////// 5. ZAYIF SINIFLARIN KALDIRILMASI ////////////////////////////////////////////
      /* workclass niteliğinde never-worked ve without-pay sınıfları ve
    occupation niteliğinde  Armed-Forces  sınıfı
      çok az tekrarlanmış. Veri setinden çıkarılabilir.
      */

      val adultWholeDF4 = adultWholeDF3
        .filter(!(col("workclass").contains("never-worked") || col("workclass").contains("without-pay") ||
          col("occupation").contains("Armed-Forces")))

      println("\n ZAYIF SINIFLARIN KALDIRILMASI  sonrası satır sayısı:: ", adultWholeDF4.count())

      // Zayıf sınıfların bulunduğu satırlar silindi.





      ///////////////////////////////// 6. EĞİTİM DURUMUYLA İLGİLİ KATEGORİLERİ BİRLEŞTİRME  ///////////////////////////////////
      /*
   education niteliğindeki:
      1st-4th, 5th-6th, 7th-8th: elementary-school
    9th, 10th, 11th, 12th: high-school
    Masters, Doctorate: high-education
    Bachelors, Some-college: undergraduate
    sınıfları yukarıdaki gibi birleştirilebilir.
    */
      adultWholeDF4.withColumn("education",
        when(col("education").contains("1st-4th") || col("education").contains("5th-6th") || col("education").contains("7th-8th"), "elementary-school")
          .when(col("education").contains("9th") || col("education").contains("10th") || col("education").contains("11th") || col("education").contains("12th"), "high-school")
          .when(col("education").contains("Masters") || col("education").contains("Doctorate"), "post-graduate")
          .when(col("education").contains("Bachelors") || col("education").contains("Some-college"), "under-graduate")
          .otherwise(col("education")).as("ed")
      ).show(30)


    // Kontrol yaptıktan sonra değişimi yeni DF'te tutalım

    /*
      1. Yöntem
      val adultWholeDF5 = adultWholeDF4.withColumn("education_merged",
        when(col("education").contains("1st-4th") || col("education").contains("5th-6th") || col("education").contains("7th-8th"), "Elementary-School")
          .when(col("education").contains("9th") || col("education").contains("10th") || col("education").contains("11th") || col("education").contains("12th"), "High-School")
          .when(col("education").contains("Masters") || col("education").contains("Doctorate"), "Post-Graduate")
          .when(col("education").contains("Bachelors") || col("education").contains("Some-college"), "Under-Graduate")
          .otherwise(col("education")))
      )
        */

// 2. Yöntem Bu yöntem boşlukları temizlemeden çalışmadı.
    val adultWholeDF5 = adultWholeDF4.withColumn("education_merged",
      when(col("education").isin("1st-4th","education","5th-6th","education","7th-8th"), "Elementary-School")
        .when(col("education").isin("9th","10th","11th","12th"), "High-School")
        .when(col("education").isin("Masters","Doctorate"), "Post-Graduate")
        .when(col("education").isin("Bachelors","Some-college"), "Under-Graduate")
        .otherwise(col("education")))



    // Birleştirme sonucunu görelim.
      println("education birleştirme sonucunu")
      adultWholeDF5.groupBy($"education_merged")
        .agg(count($"*").as("sayi"))
        //.count()
        .show(false)

    // Evet yeni bir sütun ekledik. education_merged. Daha sade.


    ///////////////////////////////// 7. native_country niteliğinde temizlik  ///////////////////////////////////
    // native_country'de ? var ve Hollanda 1 kez tekrarlanmış.
    // ? temizleme işini 4. aşamada hallettik.
    // Burada sadece Hollandayı (Holand-Netherlands) çıkaralım.

    val adultWholeDF6 = adultWholeDF5.filter(!(col("native_country").contains("Holand-Netherlands")))

    // Kontrol
    println("Hollanda öncesi satır sayısı: ",adultWholeDF5.count())
    println("Hollanda sonrası satır sayısı: ", adultWholeDF6.count())

    /*
    (Hollanda öncesi satır sayısı: ,30153)
    (Hollanda sonrası satır sayısı: ,30152)
     Satır bir azaldığına göre çıkarma işlemi başarılı.
     */

    ///////////////////////////// 8. OUTPUT . KALDIRMA ///////////////////////////////////////////
    // Bu temizliği 2. aşamada yapmıştık.


    ///////////////////////////// 9. TEMİZ VERİYİ DİSKE YAZALIM /////////////////////////////////
   // Sütun sırasını değiştirelim. education_merge'i education yanına alalım
   val nitelikSiralama = Array[String]("workclass", "education", "education_merged", "marital_status", "occupation", "relationship", "race",
     "sex", "native_country", "age", "fnlwgt", "education_num", "capital_gain", "capital_loss", "hours_per_week","output")

   val adultWholeDF7 = adultWholeDF6.select(nitelikSiralama.head,nitelikSiralama.tail:_*)

    // Diske yazma
    adultWholeDF7
      .coalesce(1) // tek parça olması için
      .write
      .mode("Overwrite")
      .option("sep",",")
      .option("header","true")
      .csv("D:\\Datasets\\adult_preprocessed")

    // Yazdığımız yerde dosyayı Notepad++ ile kontrol edelim
    // Nitelik sırası düzgün ve satır sayısı da 30153 (başlık dahil) sorun yoktur.
    }
  }
