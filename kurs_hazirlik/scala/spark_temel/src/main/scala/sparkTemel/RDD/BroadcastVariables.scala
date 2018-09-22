package sparkTemel.RDD

import org.apache.spark.SparkContext
import org.apache.log4j.{Logger,Level}

object BroadcastVariables {
  def main(args: Array[String]): Unit = {
  Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[4]","BroadcastVariables")

    println("\nDepartments: ")
    val departmentsRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\data\\retail_db\\departments.csv")
    departmentsRDD.take(5).foreach(println(_))

    println("\nCategories: ")
    val categoriesRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\data\\retail_db\\categories.csv")
    categoriesRDD.take(5).foreach(println(_))

    println("\nProducts: ")
    val productsRDD = sc.textFile("C:\\Users\\toshiba\\SkyDrive\\veribilimi.co\\udemy-apache-spark\\data\\retail_db\\products.csv")
    productsRDD.take(5).foreach(println(_))







    sc.stop()
  }
}
