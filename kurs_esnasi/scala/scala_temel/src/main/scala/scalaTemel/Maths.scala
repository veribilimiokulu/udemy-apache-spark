package scalaTemel

import scala.math._
object Maths {
  def main(args: Array[String]): Unit = {
    println("100 + 500 =" + (100+500))

    println("5 mod 2 = " + 5%2)

    /**************  RAKAM ARTTIRMA VE AZALTMA *********/
    var myNumber = 1000
    myNumber += 1
    println("myNumber 1 arttı: " + myNumber)
    myNumber -= 1
    println("myNumber 1 azaldı: " + myNumber)
    myNumber *= 2
    println("myNumber 2 ile çarpıldı: " + myNumber)

    /**************  MUTLAK DEĞER *********/
    println("-8'in mutlak değeri: " + abs(-8))

    /**************  KAREKÖK VE ÜSTEL SAYI *********/
    println("8'in karekökü: " + sqrt(8))
    println("2'nin 8'inci kuvveti: " + pow(2,8))
    println(exp(1))

    /**************  ONDALIKLI SAYIYI YUVARLAMA *********/
    println("Round: " + round(2.54))
    println("Round: " + floor(2.99))
    println("Round: " + ceil(2.04))

    /**************  LOGARITMA *********/
    println("log(2): " + log(2))  // e tabanında

    println("log10(10) : " + log10(10))

    /**************  MIN MAX *********/
    println("2 ve 5 küçük olan: " + min(2,5))


  }
}
