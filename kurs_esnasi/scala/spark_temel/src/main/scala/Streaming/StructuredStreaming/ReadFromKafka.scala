package Streaming.StructuredStreaming

import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import org.apache.spark.sql.streaming.Trigger

object ReadFromKafka {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("ReadFromKafka")
      .getOrCreate()


    import  spark.implicits._


    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe","deneme")
      .load()
/*
+----+--------------------+------+---------+------+--------------------+-------------+
| key|               value| topic|partition|offset|           timestamp|timestampType|
+----+--------------------+------+---------+------+--------------------+-------------+
|null|[6D 65 72 68 61 6...|deneme|        0|    30|2019-05-17 23:53:...|            0|
+----+--------------------+------+---------+------+--------------------+-------------+
 */

val df2 = df.select($"key".cast(StringType), $"value".cast(StringType))

/*
+----+-------------+
| key|        value|
+----+-------------+
|null|merhaba spark|
+----+-------------+

 */
val df3 = df2.select($"value").as[String]
  .flatMap(_.split("\\W+"))
  .groupBy("value").count()
  .sort(f.desc("count"))




    val query = df3.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    query.awaitTermination()
  }
}
