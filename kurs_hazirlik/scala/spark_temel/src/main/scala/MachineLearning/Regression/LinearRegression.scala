package MachineLearning.Regression

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SparkSession
object LinearRegression {
  def main(args: Array[String]): Unit = {
    //********* LOG SEVİYESİNİ AYARLAMA ************************
    Logger.getLogger("org").setLevel(Level.ERROR)

    //********* SPARK SESSION OLUŞTURMA ************************
    val spark = SparkSession.builder()
      .appName("LinearRegression")
      .master("local[4]")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    //********* VERİ SETİNİ OKUMA ************************
    // veriyi okuyarak dataframe oluşturma
    // Veri hakkında kısa bilgi: Dünya sağlık örgütünün çeşitli faktörlere göre ortalama yaşam süresini (life expectancy)
    // tahmin etmede kullanılan veriseti. Ön işleme ve temizlik yapılmış.
    // Veri kaynağı: https://www.kaggle.com/kumarajarshi/life-expectancy-who
    val df = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .option("inferSchema","true")
      .load("D:\\Datasets\\LifeExpectancyData.csv")

    //********* VERİ SETİNİ ANLAMAK VE KEŞFETMEK ************************
    // okunan dataframe'e ilk bakış
    println("\n Orijinal DF")
    df.show(20)

    // Okuma kontrolü yapıldıktan sonra veri seti Kaggle'dan daha detaylı incelenir
    //https://www.kaggle.com/kumarajarshi/life-expectancy-who

    //********* SÜTUN İSİMLERİNDEKİ BOŞLUKLARI KALDIRMAK VE YENİDEN İSİMLENDİRMEK ************************
    //df.printSchema()
    //df.columns.foreach(println(_))
    val yeniSutunIsimleri = Array("Country", "Year", "Status", "LifeExpectancy", "AdultMortality", "InfantDeaths", "Alcohol",
     "PercentageExpenditure", "HepatitisB", "Measles", "BMI", "Under-fiveDeaths", "Polio", "TotalExpenditure",
     "Diphtheria", "HIV_AIDS", "GDP", "Population", "Thinness1-19Years", "Thinness5-9Years", "IncomeCompositionOfResources","Schooling")

    val df2 = df.toDF(yeniSutunIsimleri:_*)

    // yeni sütun isimlerini ve değerlerin doğruluğunu görelim
    df2.show()




    //********* VERİ SETİNİ EĞİTİM VE TEST OLARAK 2'YE AYIRMAK ************************

    // Veri setini train ve test olarak ayırma
    val Array(trainDF, testDF) = df.randomSplit(Array(0.75, 0.25),142L)


    // Ayrılan setleri kontrol etmek
    println("trainDF: ")
    //trainDF.show(5)
    println("testDF: ")
   // testDF.show(5)


    /*
        val lr = new LinearRegression()
          .setMaxIter(10)
          .setRegParam(0.3)
          .setElasticNetParam(0.8)

        // Fit the model
        val lrModel = lr.fit(trainDF)

        // Print the coefficients and intercept for linear regression
        println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        // Summarize the model over the training set and print out some metrics
        val trainingSummary = lrModel.summary
        println(s"numIterations: ${trainingSummary.totalIterations}")
        println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        trainingSummary.residuals.show()
        println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        println(s"r2: ${trainingSummary.r2}")
      */


  }

}
