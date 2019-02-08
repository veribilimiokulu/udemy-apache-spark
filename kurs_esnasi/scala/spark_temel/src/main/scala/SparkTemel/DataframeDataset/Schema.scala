package SparkTemel.DataframeDataset

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.LongType

object Schema {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("Schema")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._



    // veriyi okuyarak dataframe oluşturma
    val dfFromFile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")

    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    dfFromFile.show(5)

    dfFromFile.printSchema()

    println("Örneğin yukarıda UnitPrice'ın  Float olmasına ihtiyaç var.\n ")

    // Spark'ın çıkarımı bize her zaman yetmez ellerimizle kendi şemamızı yapalım
    // Önce gerekli kütüphaneleri içeri alalım

    import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, FloatType, DateType,DoubleType}

    val retailManualSchema = new StructType(
      Array(
        new StructField("InvoiceNo", StringType, true),
        new StructField("StockCode", StringType, true),
        new StructField("Description", StringType, true),
        new StructField("Quantity", IntegerType, true),
        new StructField("InvoiceDate", StringType, true),
        new StructField("UnitPrice", FloatType, true),
        new StructField("CustomerID", IntegerType, true),
        new StructField("Country", StringType, true)
      )
    )

    // Veriyi tekrar okuyalım ancak bu sefer inferSchema() yerine elle tanımladığımız şemayı kullanalım
    val dfFromFile2 = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .schema(retailManualSchema)
      .load("D:\\Datasets\\OnlineRetail.csv")

    // elle tanımlı şema ile oluşmuş dataframe'e göz atalım
    println("\n elle tanımlı şema ile oluşmuş dataframe'e göz atalım")
    dfFromFile2.show(5)

    // elle tanımlı şemaya göz atalım
    println("\n elle tanımlı şemaya göz atalım")
    dfFromFile2.printSchema()

    println("############## ÇÖZÜM SONRASI ############################")
    // UnitPrice'da hala sıkıntı var. . yerine , olduğu için string'den float'a dönüştüremiyor.
    // Çözüm olarak veriyi okurken , ile .'yı değiştirerek okuyalım ve dataframe'i tekrar , . değişmiş
    // şekilde diske yazalım
    val dfFromFile3 = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")
      .withColumn("UnitPrice",regexp_replace($"UnitPrice", ",",".")) // okurken , . yer değiş


    // , . yer değişmiş dataframe'i diske csv olarak yaz.
    dfFromFile3
      .coalesce(1) // tek parça olması için
      .write
      .mode("Overwrite")
      .option("sep",";")
      .option("header","true")
      .csv("D:\\Datasets\\OnlineRetail2")


    // düzeltilmiş csv'yi tekrar oku elle tanımlanan şemayı ver
    val dfFromManualSchema = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      //.option("inferSchema","true")
      .schema(retailManualSchema)
      .load("D:\\Datasets\\OnlineRetail2")

    println("\n Elle şema oluşturduktan sonra")
    dfFromManualSchema.printSchema()

    println("\n Yeni şemaya sahip dataframe görünüş")
    dfFromManualSchema.show(5)


  }
}
