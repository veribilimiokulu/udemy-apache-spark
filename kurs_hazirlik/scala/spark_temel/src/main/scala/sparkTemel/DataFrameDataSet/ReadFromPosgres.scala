package sparkTemel.DataFrameDataSet

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}

object ReadFromPosgres {
  def main(args: Array[String]): Unit = {
    /*
    pom.xml dosyasına aşağıdaki dependency'i eklemeyi unutmayın
    <!-- https://mvnrepository.com/artifact/org.postgresql/postgresql -->
        <dependency>
            <groupId>org.postgresql</groupId>
            <artifactId>postgresql</artifactId>
            <version>9.4.1207</version>
        </dependency>
     */


    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("ReadFromPosgres")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext

    import spark.implicits._

    //********* POSGRES'DEN OKUMA ************************
    val posgresDF = spark.read
      .format("jdbc")
      .option("driver","org.postgresql.Driver")
      .option("url","jdbc:postgresql://docker:5432/spark")
      .option("dbtable","simple_data")
      .option("user","postgres")
      .option("password","postgres")
      .load()


    posgresDF.show()

  }
}
