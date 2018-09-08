package ScalaTemel

object Strings {
  def main(args: Array[String]): Unit = {

    var cumle = "Ali ata bak."

    // stringe ait bir elemana indis kullanarak ulaşma
    println("4. indis : " + cumle(4)) // Sonuç: a

    // for döngüsü ile string elemanları içinde dolaşma
    for(harf <- cumle) println(harf)

    // substring
    println(cumle.substring(3,5)) // Sonuç: a

    // İçerik konrolü
    println(cumle.contains("i")) // içeriyorsa true değilse false
    println(cumle.contains("ş")) // içeriyorsa true değilse false


    // İki stringi birleştirme
    println(cumle.concat(" Emel eve gel."))

    // -ile bitiyor mu false true
    println(cumle.endsWith(".")) // Sonuç: true


    // Hshcode alma
    println(cumle.hashCode) // Sonuç: 2069860438

    // string boyutu
    println(cumle.length) // Sonuç: 12

    // string içinde bul ve değiştir
    println(cumle.replace("Ali","Mehmet")) //Sonuç: Mehmet ata bak.



  }
}
