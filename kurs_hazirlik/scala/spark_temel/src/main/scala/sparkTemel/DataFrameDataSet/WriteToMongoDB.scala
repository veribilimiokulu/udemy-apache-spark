package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark


object WriteToMongoDB {
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

    // Read data from disk
    val df = spark.read.format("csv")
      .option("header",true)
      .option("inferSchema",true)
      .option("sep",",")
      .load("D:/Datasets/simple_data.csv")

    // Show the data
    df.show()
    val writeConfig = WriteConfig(Map("collection" -> "spark", "writeConcern.w" -> "majority"), Some(WriteConfig(spark.sparkContext)))
    MongoSpark.save(df, writeConfig)
  }
}
