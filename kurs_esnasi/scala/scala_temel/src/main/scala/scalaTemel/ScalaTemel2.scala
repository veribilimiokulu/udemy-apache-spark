package scalaTemel

object ScalaTemel2 {
  def main(args: Array[String]): Unit = {
    println("Merhaba ScalaTemel2")

    def ikiSayininCarpimi(x:Int, y:Int): String ={
      val sonuc = x*y

      return sonuc.toString
    }

    println(ikiSayininCarpimi(3,5))

   class Insan(isim:String, yas:Int){
     def kendiniTanit():Unit={
       println("Adım: "+ isim + " yaşım: " + yas)
     }
   }

    val erkan = new Insan("Erkan",40)

    erkan.kendiniTanit()

    val mehmet = new Insan("Mehmet", 35)

    mehmet.kendiniTanit()

  }
}
