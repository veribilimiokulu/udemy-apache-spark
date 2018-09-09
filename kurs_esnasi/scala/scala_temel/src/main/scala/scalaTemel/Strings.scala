package scalaTemel

object Strings {
  def main(args: Array[String]): Unit = {
    var cumle = "Ali ata bak."

    // stringe ait bir elemana indis kullanarak ulaşma
    println("4. indis : " + cumle(4)) // Sonuç: a


    // for döngüsü ile string elemanları içinde dolaşma
    for(harf <- cumle) println(harf)

    // substring
    println(cumle.substring(2,5)) // Sonuç: i a (son indis harç

    // İçerik konrolü
    println(cumle.contains("i")) // içeriyorsa true değilse false
    println(cumle.contains("ş")) // içeriyorsa true değilse false


    // İki stringi birleştirme
    val emelinFisi = " Emel eve gel."
    //println(cumle.concat(emelinFisi))

    val aliEmelFisi = cumle.concat(emelinFisi)
    println(aliEmelFisi)

    // -ile bitiyor mu false true
    println(cumle.endsWith(".")) // Sonuç: true

    /// Hshcode alma
    println(cumle.hashCode) // Sonuç: 2069860438

    // string boyutu
    println(cumle.length) // Sonuç: 12
    println(aliEmelFisi.length) // 26


    // string içinde bul ve değiştir
    println(cumle.replace("Ali","Mehmet")) //Sonuç: Mehmet ata bak.
    println(aliEmelFisi.replace("Emel","Oya")) //Sonuç: Ali ata bak. Oya eve gel.


  }
}
