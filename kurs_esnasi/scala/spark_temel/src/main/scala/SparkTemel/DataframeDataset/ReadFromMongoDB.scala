package SparkTemel.DataframeDataset
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}
import com.mongodb.spark.config._
import com.mongodb.spark.MongoSpark
object ReadFromMongoDB {
  def main(args: Array[String]): Unit = {
    // Set Log Level
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
      .appName("ReadFromMongoDB")
      .master("local[4]")
      .config("spark.mongodb.input.uri", "mongodb://192.168.99.107:27017/test.myCollection") // ip adresi docker-machine default adresi
      .config("spark.mongodb.output.uri", "mongodb://192.168.99.107:27017/test.myCollection")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    // Read from mongodb

    val readConfig = ReadConfig(Map("collection" -> "spark", "readPreference.name" -> "secondaryPreferred"),
      Some(ReadConfig(spark.sparkContext)))

    val simple_dataRDD = MongoSpark.load(spark.sparkContext, readConfig)

    val df = simple_dataRDD.toDF()

    df.show()
  }
}
