package ScalaTemel

import scala.util.Random

object RandomNumbers {
  def main(args: Array[String]): Unit = {

    // Random nesnesi yaratma
    val r = Random

    // Rastgele bir Int üretme
    println(r.nextInt())

    // Rastgele üretilecek sayıya sınır koyma (0-99 arasında bir sayı üret)
    println(r.nextInt(100))

    // random float üretme (0 ile 1 arasında değer döner)
    println(r.nextFloat)

    //random Double (0 ile 1 arasında değer döner)
    println(r.nextDouble)



    // Seed belirleyerek çalışmaların tekrarı sağlanır
    r.setSeed(1000L)
    println(r.nextInt(100)) // Yukarıdaki int sürekli değişirken buradaki aynı değer üretiyor


    println(r.nextPrintableChar())

  }
}
