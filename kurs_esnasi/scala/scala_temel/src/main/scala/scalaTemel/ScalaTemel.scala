package scalaTemel

object ScalaTemel {
  def main(args: Array[String]): Unit = {
    println("Merhaba Scala")

    println(1+1)


    println("Merhaba"+ ", " + "Scala.")

    val myVal = 10
    //myVal =20

    var myVar = 20
    myVar = 30

    println("myVar değeri: "+ myVar)

    val cumle = "Benim cümlem"
    var sayac = 0
    cumle.foreach(x => {
      println(x.toUpper)
      sayac += 1

    })
    println(sayac)


    val ikiSayiTopla = (x:Int, y:Int) => x + y

    println("İki sayının fonksiyon ile toplanması: " + ikiSayiTopla(5,3))


  }
}
