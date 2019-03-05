package Streaming.StructuredStreaming
/*
Erkan ŞİRİN
Spark Structured Streaming ve csv file stream kullanılarak şema yaratma ve
mesleklere göre aylık gelirleri büyükten küçüğe sıralama
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromCsv {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromCsv")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._

    val mySchema = new StructType()
      .add("sirano", IntegerType)
      .add("isim", StringType)
      .add("yas",IntegerType)
      .add("meslek",StringType)
      .add("sehir",StringType)
      .add("aylik_gelir",DoubleType)


    // Satırları oku
    val df = spark.readStream
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .schema(mySchema)
      .load("D:\\spark-streaming-test")

    val meslekOrtGelir = df.groupBy("meslek")
      .agg(avg("aylik_gelir").as("ortGelir"))
      .sort(desc("ortGelir"))

    // Streaming başlasın
    val query = meslekOrtGelir.writeStream
      .outputMode("complete") //sorguda aggregation varsa complete
      .format("console")
      .start()

    query.awaitTermination()

/*
Orijinal dosyaya iki ilave kayıt daha gönderdiğimizde aldığımız sonuç:
-------------------------------------------
Batch: 0
-------------------------------------------
+-----------+------------------+
|     meslek|          ortGelir|
+-----------+------------------+
|     Doktor|           16125.0|
|     Berber|           12000.0|
|   Müzisyen|            9900.0|
|Pazarlamaci| 5433.333333333333|
| Tuhafiyeci|            4800.0|
|    Tornacı|            4200.0|
|      Memur|4066.6666666666665|
|       Isci|            3500.0|
+-----------+------------------+

-------------------------------------------
Batch: 1
-------------------------------------------
+-----------+------------------+
|     meslek|          ortGelir|
+-----------+------------------+
|     Doktor|           16125.0|
|     Berber|           12000.0|
|   Müzisyen|            9300.0|
|Pazarlamaci| 5433.333333333333|
| Tuhafiyeci|            4800.0|
|    Tornacı|            4200.0|
|      Memur|4066.6666666666665|
|       Isci|            3500.0|
+-----------+------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+-----------+------------------+
|     meslek|          ortGelir|
+-----------+------------------+
|     Doktor|           16125.0|
|   Müzisyen|            9300.0|
|     Berber|            6050.0|
|Pazarlamaci| 5433.333333333333|
| Tuhafiyeci|            4800.0|
|    Tornacı|            4200.0|
|      Memur|4066.6666666666665|
|       Isci|            3500.0|
+-----------+------------------+
 */

  }
}
