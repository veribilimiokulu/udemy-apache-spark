package ScalaTemel

import math._
import scala.collection.immutable.Range
import scala.util.Random

object DataTypes {
  def main(args: Array[String]): Unit = {

    /*******  BYTE  *********/
    // -128 ile 127 arası tam sayı
    println("Byte, " + (-pow(2,7)) + " ile " + (pow(2,7) - 1) +" arasında tamsayı değer alır.")

    val bayt:Byte = 100 // hata yok
    //val bayt:Byte = 129 // hata verir




    /*******  BOOLEAN  *********/
    // true veya false değerlerini alır
    println(1 == 2) // false yazdırır





    /*******  CHAR  *********/
    // unsigned max value 65535 kadar karakter alır
    //val c:Char = ': !' // hata verir
    val b:Char = '='
    println(b)





    /*******  SHORT  *********/
    //-32768 ile 32767 arasında tamsayı değer alır
    println("Short, " + (-pow(2,15)) + " ile " + (pow(2,15) - 1) +" arasında tamsayı değer alır.")
    println("Short, " + Short.MinValue + " ile " + Short.MaxValue +" arasında tamsayı değer alır.")

    val myShort:Short = 32767
    //val myShort2:Short = 38000 // hata verir



    /*******  INT  *********/
    // -2.147483648E9 ile 2.147483647E9 arası değer alır
    println("Int, " + (-pow(2,31)) + " ile " + (pow(2,31) - 1) +" arasında tamsayı değer alır.")
    println("Int, " + Int.MinValue + " ile " + Int.MaxValue +" arasında tamsayı değer alır.")




    /*******  LONG  *********/
    // -9.223372036854776E18 ile 9.223372036854776E18 arası değer alır
    println("Long, " + (-pow(2,63)) + " ile " + (pow(2,63) - 1) +" arasında tamsayı değer alır.")
    println("Long, " + Long.MinValue + " ile " + Long.MaxValue +" arasında tamsayı değer alır.")


    /*******  FLOAT  *********/
    // 3.4028234663852886E38 ile 3.4028234663852886E38 arası değer alır
    println("Float, " + ((2 - pow(2,-23)) * pow(2,127)) + " ile " + ((2 - pow(2,-23)) * pow(2,127)) +" arasında ondalıklı değer alır.")
    println("Float, " + Float.MinValue + " ile " + Float.MaxValue +" arasında tamsayı değer alır.")


    /*******  DOUBLE  *********/
    // -9.223372036854776E18 ile 9.223372036854776E18 arası değer alır
    println("Double, " + Double.MinValue + " ile " + Double.MaxValue +" arasında ondalıklı değer alır.")


    /*******  BIGINT  *********/
    // Çok büyük rakamlar için
    println("BigInt: " + BigInt.apply("123456789987456321234569874561200"))
    var myBigInt = BigInt("123456789987456321234569874561200")
    println("myBigInt'e bir ekledim: " + myBigInt + 1)


    /*******  BIGDECIMAL  *********/
    // Çok büyük ondalıklı sayılar için için
    println("BigInt: " + BigDecimal("1.23456789987456321234569874561200"))
    var myBigDec = BigDecimal("1.23456789987456321234569874561200")
    println("myBigDec'e bir ekledim: " + myBigDec + 0.00000000000000000000000000000001)


    /**************  TÜR DÖNÜŞÜMLERİ *********/

    /*
    Byte -> Short -> Int -> Long -> Float -> Double
     */
    val dbMax = Double.MaxValue
    println("DoubleMax: "+ dbMax + " Double to Int: " + dbMax.toInt)

    val intMax = Int.MaxValue
    println("IntMax: "+ intMax + " Int to Double: " + intMax.toDouble)

    // Büyük veriden küçüğe dönüşümde truncate oluşur dikkat etmek gerekir

  }
}
