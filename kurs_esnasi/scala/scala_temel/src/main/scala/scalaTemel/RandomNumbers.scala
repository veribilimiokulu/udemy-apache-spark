package scalaTemel


import scala.util.Random

object RandomNumbers {
  def main(args: Array[String]): Unit = {

    // Random nesnesi yaratma
    val r = Random

    r.setSeed(1001L)

    // Rastgele bir Int üretme
    println(r.nextInt)

    // Rastgele Int belli bir sınır içinde
    println("Belirli bir sınır içinde Int üret: " + r.nextInt(100))

    // random float üretme (0 ile 1 arasında değer döner)
    println("Rastgele float: " + r.nextFloat)

    // random double üretme (0 ile 1 arasında değer döner)
    println("Rastgele double: " + r.nextDouble)


    println(r.nextPrintableChar())

    println(r.nextString(10))

  }
}
