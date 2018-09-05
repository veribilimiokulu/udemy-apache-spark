package sparkTemel

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object PivotTable {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    println("Pivot Table Örneği")

    // Spark Session oluşturma
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("RDD-Olusturmak")
      .config("spark.executor.memory","4g")
      .config("spark.driver.memory","2g")
      .getOrCreate()

    import spark.implicits._

    println("/************************** HATA MATRİSİ İLE ÖRNEK ************************************/")
    // Hata matrisi için yapay veri oluşturma
    val matrisDF = spark
                  .createDataset(Seq(
                    (1,0),
                    (1,1),
                    (0,0),
                    (0,1),
                    (1,0),
                    (1,1),
                    (0,0),
                    (0,1),
                    (1,1),
                    (1,1),
                    (0,0)
                  ))
        .selectExpr("_1 as label","_2 as prediction") // sütun isimlerilabel ve prediction olarak değiştir

    // Hata matrisi datasetine göz atma
    matrisDF.show()

    // Pivota geçmeden önce gruplanmış haline bakalım
    matrisDF.groupBy("label","prediction").count().show()

    // Şimdi pivot tablosu oluşturma 0'lar yukarıda
    val matrisDFGrouped =  matrisDF.groupBy("label")
      .pivot("prediction", (0 to 1)) // pivotlanacak sütun ve alacağı değerler
      .count() // gruplama fonksiyon
      .orderBy($"label") // küçükten büyüğe sıralayalım çünkü pivotun değerleri de küçükten büyüğe (0 to 1)

    println("1 aşağıda matris")
    // 1 aşağıda hata matrisi
    matrisDFGrouped.show()


    val matrisDFGrouped2 =  matrisDF.groupBy("label") //
      .pivot("prediction", (1 to 0 by -1)) // pivotlanacak sütun ve alacağı değerler büyükten küçüğe 1 yukarıda olsun diye
      .count() // gruplama fonksiyon
      .orderBy($"label".desc) // büyükten küçüğe sıralayalım çünkü pivotun değerleri de büyükten küçüğe (1 to 0)





    println("1 yukarıda matris")
    // 1 yukarıda hata matrisi
    matrisDFGrouped2.show()




  }
}
