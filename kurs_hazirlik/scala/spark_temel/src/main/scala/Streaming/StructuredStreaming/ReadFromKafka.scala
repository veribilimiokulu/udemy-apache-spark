package Streaming.StructuredStreaming
/*
Erkan ŞİRİN
Spark Structured Streaming ve kafka kullanılarak wordcount uygulaması
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
/*
pom.xml dosyası dependencies altına
      <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-sql-kafka-0-10_2.11</artifactId>
            <version>2.3.1</version>
        </dependency>
        eklemeyi unutmayın
 */
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromKafka")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._


    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "deneme")
      .load()
/*
df ham hali
+----+--------------------+------+---------+------+--------------------+-------------+
| key|               value| topic|partition|offset|           timestamp|timestampType|
+----+--------------------+------+---------+------+--------------------+-------------+
|null|[4E 61 73 69 6C 2...|deneme|        0|     5|2019-03-04 22:11:...|            0|
+----+--------------------+------+---------+------+--------------------+-------------+
 */

    // Ham dataframe'in key ve value sütunlarını stringe çevir
    val df2 = df.select(col("key").cast(StringType), col("value").cast(StringType))

    // value sütununu boşluklarından ayır ve kelime sayısını tespit et.
    val df3 = df2.select("value").as[String].flatMap(_.split(" "))
      .groupBy(col("value")).count()
      .sort(desc("count"))

    // Streaming başlasın
    val query = df3.writeStream
      .outputMode("complete") //sorguda aggregation varsa complete
      .format("console")
      .start()

    query.awaitTermination()


  }
}
