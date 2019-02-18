package SparkTemel.DataframeDataset
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._

object StringOps {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)


    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("StringOps")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .config("spark.executor.memory", "4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("sep", ",")
      .option("inferSchema", "true")
      .load("D:\\Datasets\\simple_dirty_data.csv")

    df.show()



    //1. concat

    val df2 = df.select("meslek", "sehir")
      .withColumn("meslek_sehir", concat(col("meslek"), lit(" - "), col("sehir")))
    //.show(truncate = false)


    // 2 . Number formaf
    // df.withColumn("aylik_gelir_format",format_number(($"aylik_gelir"), 2))
    // .show()


    // 3 . lower, initcap, length

    df.withColumn("meslek_lower", lower(col("meslek")))
      .withColumn("isim_initcap", initcap($"isim"))
      .withColumn("sehir_length", length(col("sehir")))
    //.show()


    // 4. trim
    df.withColumn("sehir_ltrim", ltrim(col("sehir")))
      .withColumn("sehir_rtrim", rtrim(col("sehir")))
      .withColumn("sehir_trim", trim(col("sehir")))
    //.show()

    // 5. replace, split
    df.withColumn("sehir_ist", regexp_replace(col("sehir"), "Ist", "İST"))
      .withColumn("mal_mulk_split", split(col("mal_mulk"), "\\|"))
      .withColumn("mal_mulk_ilk", col("mal_mulk_split")(0))
      .show()

/*
    //********* SPARK SESSION OLUŞTURMA ************************//
    val spark = SparkSession.builder()
      .appName("Schema")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._


    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\simple_dirty_data.csv")

    println("\nDataframe ilk görünüş:")
    df.show()
    /******************************************************/

    // Bu ders tamamen sql string ops ile ilgili o yüzden hepsini indirelim
    import org.apache.spark.sql.functions._



    // 1. Concat
    // Meslek ve şehir birleştirme. Araya birşey eklemek için lit() fonksiyonu kullanırız.
    val dfConcat = df.withColumn(
      "meslek_sehir", concat(col("meslek"),lit(" - "), col("sehir"))
    )
    //dfConcat.show(3)


    // 2. Number format
    val dfNumberFormat = df.withColumn(
      "aylik_gelir_format", format_number(col("aylik_gelir"), 2)
    )
    //dfNumberFormat.show(3)


    // 3. lower, initcap, length
    val dfLower = df.withColumn("meslek_lower", lower('meslek))
      .withColumn("isim_initcap", initcap('isim))
      .withColumn("sehir_length", length($"sehir"))
    //dfLower.show()


    // 4. trim
    val dfTrim = df.withColumn("sehir_rtrim", rtrim(col("sehir")))
      .withColumn("sehir_ltrim", ltrim(col("sehir")))
      .withColumn("sehir_trim", trim(col("sehir")))
    //dfTrim.show()


    // 5. replace, split
    val dfReplace = df.withColumn("sehir_ist", regexp_replace(col("sehir"), "Ist","İST"))
      .withColumn("mal_mulk_split", split(col("mal_mulk"), "\\|"))
      .withColumn("mal_mulk_ilk",col("mal_mulk_split")(0))
    dfReplace.show(false)

    dfReplace.printSchema()
*/

  }
}





