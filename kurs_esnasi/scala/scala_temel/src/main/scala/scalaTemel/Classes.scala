package scalaTemel

object Classes {
  def main(args: Array[String]): Unit = {

    val selamSabah1 = new Selam("Merhaba, ", "!")
    selamSabah1.sabah("Turgut Amca") // Merhaba, Turgut Amca!


    val selamSabah2 = new Selam("Hoşgeldiniz ", " yine bekleriz.")
    selamSabah2.sabah("Ayşe Teyze")


    /************* CASE CLASS ***************/
    case class Point(x: Int, y: Int)

    val point1 = Point(1,2)
    val point2 = Point(2,3)

    if(point1 == point2) println("Aynı")  else println("Farklı")


    case class Ogrenci(isim:String, notu:Float)
    val ahmet = Ogrenci("Ahmet", 89.77F)

    println(s"${ahmet.isim}'in başarı notu: ${ahmet.notu}.")

    case class Calisan(isim:String="İsim yok", unvan:String="İsim Yok", maas:Int=3000)
    val sevda = Calisan("Sevda","Analist", 5000)

    println(s"${sevda.isim}, ${sevda.unvan} pozisyonunda aylık ${sevda.maas} TL maaş ile çalışıyor.")

  }
}
