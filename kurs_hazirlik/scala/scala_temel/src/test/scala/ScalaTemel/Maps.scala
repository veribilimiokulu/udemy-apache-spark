package ScalaTemel

object Maps {
  def main(args: Array[String]): Unit = {
    // collection, immutable, anahtar-değer,


    /****************  IMMUTEABLE MAP   *************************/
    val ulkeBaskent = Map("Japonya"->"Tokyo",  //immutable
      "Hindistan" -> "Delhi",
      "Güney Kore" -> "Seul")

    // if ile eğer bir ülke Map içinde var ise başkentini yazdırsın
    var anahtar = "Almanya"
    if(ulkeBaskent.contains(anahtar)){
      println(ulkeBaskent(anahtar))
    }else{
      println(s"Aranılan ${anahtar}, ülkeler içinde yok.")
    }



    // ulkeBaskent("Almanya") = "Berlin" // immutable.Map olduğu için hata verir



    /****************  MUTEABLE MAP   *************************/
    val ulkeBaskentMut = collection.mutable.Map("Japonya"->"Tokyo",  //muteable
      "Hindistan" -> "Delhi",
      "Güney Kore" -> "Seul")

    ulkeBaskentMut("Almanya") = "Berlin" // mutable.Map olduğu için kabul eder

    // if ile eğer bir ülke Map içinde var ise başkentini yazdırsın.
    // Almanya'yı ekleyebildiğimize göre
    if(ulkeBaskentMut.contains(anahtar)){
      println(ulkeBaskentMut(anahtar))
    }else{
      println(s"Aranılan ${anahtar}, ülkeler içinde yok.")
    }



    val ogrenciler = collection.mutable.Map(1503 -> "Salih",
      1504 -> "Hasan",
      1505 -> "Tuncay")
    ogrenciler(1506) = "Mustafa"

    // For döngüsü ile anahtar değerleri yazdırmak.
    for((anahtar, deger) <- ogrenciler){
      printf("%d : %s \n", anahtar, deger)
    }






  }
}
