package scalaTemel

import math._
import scala.collection.immutable.Range
import scala.util.Random

object DataTypes {
  def main(args: Array[String]): Unit = {
    println("Byte, " + (-pow(2,7)) + " ile " + (pow(2,7) - 1) +" arasında tamsayı değer alır.")
    println("Byte " + Byte.MinValue + " ile " + Byte.MaxValue + " arasında değer alır.")

  val myByte:Byte = 127

    /*******  BOOLEAN  *********/
    // true veya false değerlerini alır
    println(1 == 2) // false yazdırır

    val b:Char = '='
    //val c:Char = 'ee'

    println("Short, " + Short.MinValue + " ile " + Short.MaxValue +" arasında tamsayı değer alır.")
    val myShort:Short = 32767
    //val myShort2:Short = 38000 // hata verir


    println("Float, " + Float.MinValue + " ile " + Float.MaxValue +" arasında tamsayı değer alır.")


    val dbMax = Double.MaxValue

    println("DoubleMax: "+ dbMax + " Double to Int: " + dbMax.toInt)

    val intMax = Int.MaxValue
    println("IntMax: "+ intMax + " Int to Double: " + intMax.toDouble)

  }
}
