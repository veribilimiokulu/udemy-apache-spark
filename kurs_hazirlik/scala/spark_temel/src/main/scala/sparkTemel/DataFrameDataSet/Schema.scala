package sparkTemel.DataFrameDataSet
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
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


    val dfFromFile = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      .option("inferSchema","true")
      .load("D:\\Datasets\\OnlineRetail.csv")

    // Infer Schema ne kadar sağlıklı?
     dfFromFile.printSchema()

    println("\nÖrneğin yukarıda UnitPrice'ın  Float olmasına ihtiyaç var.")

    // Spark'ın çıkarımı bize her zaman yetmez ellerimizle kendi şemamızı yapalım
    // Önce gerekli ütüphaneleri içeri alalım
    import org.apache.spark.sql.types.{StructType,StructField, StringType, IntegerType, FloatType, DateType}

    val retailManualSchema = new StructType(
      Array(
        new StructField("InvoiceNo", StringType, true),
        new StructField("StockCode", StringType, true),
        new StructField("Description", StringType, true),
        new StructField("Quantity", IntegerType, true),
        new StructField("InvoiceDate",StringType,true),
        new StructField("UnitPrice", StringType, true),
        new StructField("CustomerID",LongType, true),
        new StructField("Country",StringType,true)
      )
    )


    val dfFromManualSchema = spark.read.format("csv")
      .option("header","true")
      .option("sep",";")
      //.option("inferSchema","true")
      .schema(retailManualSchema)
      .load("D:\\Datasets\\OnlineRetail.csv")

    dfFromManualSchema.show()

  }
}
