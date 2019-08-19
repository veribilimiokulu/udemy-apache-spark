package com.veribilimiokulu

import org.apache.spark.{SparkContext}
object SparkDeneme {
  def main(args: Array[String]): Unit = {
    println("Merhaba")

    val sc = new SparkContext("local[4]", "SparkDeneme")

    val myRDD = sc.parallelize(List(1,2,3,4,5,6,7,8))

    myRDD.take(8).foreach(println(_))
  }
}
