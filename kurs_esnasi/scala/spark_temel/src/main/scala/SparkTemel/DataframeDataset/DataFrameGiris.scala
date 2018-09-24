package SparkTemel.DataframeDataset

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object DataFrameGiris {
  def main(args: Array[String]): Unit = {
     Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataFrameGiris")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    // Listeden
    val dfFromList = sc.parallelize(List(1,2,3,4,5,6,4,5)).toDF("rakamlar")
   // dfFromList.printSchema()


    // spark.range ile
    val dfFramSpark = spark.range(10,100,5).toDF("besli")
    //dfFramSpark.printSchema()

    // Dosyadan veri okuyarak

    val dfFromFile = spark.read.format("csv")
      .option("sep",";")
      .option("header","true")
      .option("inferSchema","true")
      .load("D:/Datasets/OnlineRetail.csv")

    // dfFromFile.printSchema()

   // dfFromFile.show(10,false)


    //********* DATAFRAME ÖRNEK TRANSFORMATION VE ACTION ************************

    // count() Action
    println("\nOnlineRetail satır sayısı: "+ dfFromFile.count())



  dfFromFile.select("InvoiceNo","Quantity")show(10)

    //dfFromFile.sort('Quantity).show(10)
    //dfFromFile.sort($"Quantity").show(10)
    dfFromFile.select("Quantity").sort(dfFromFile.col("Quantity")).show(10)

    // Dinamik conf ayarı ve shuffle partition sayısını değiştirme
    spark.conf.set("spark.sql.shuffle.partitions","15")

    // explain(): Bu işi nasıl yapacağını bana bir anlat
    dfFromFile.sort($"Quantity").explain()



  }
}
