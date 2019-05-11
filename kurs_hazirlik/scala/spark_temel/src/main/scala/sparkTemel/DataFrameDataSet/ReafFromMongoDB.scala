package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark

object ReafFromMongoDB {
  def main(args: Array[String]): Unit = {
      // Set Log Level
      Logger.getLogger("org").setLevel(Level.ERROR)

      // Create SparkSession
      val spark = SparkSession.builder()
        .appName("WriteToMongoDB")
        .master("local[4]")
        .config("spark.mongodb.input.uri", "mongodb://192.168.99.107:27017/test.myCollection") // ip adresi docker-machine default adresi
        .config("spark.mongodb.output.uri", "mongodb://192.168.99.107:27017/test.myCollection")
        .config("spark.driver.memory","2g")
        .config("spark.executor.memory","4g")
        .getOrCreate()

    // Read From MongoDB
      val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"),
        Some(ReadConfig(spark.sparkContext)))
      val customRdd = MongoSpark.load(spark.sparkContext, readConfig)

    val df = customRdd.toDF()
    df.show()

  }
}
