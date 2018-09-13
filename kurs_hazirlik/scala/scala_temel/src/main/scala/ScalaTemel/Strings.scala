package ScalaTemel

object Strings {
  def main(args: Array[String]): Unit = {

    // Java String ile aynı

    // Bir String oluştur
    var cumle = "Ali ata bak."

    // Sınıf adını yazdır
    println(cumle.getClass.getName)

    // stringe ait bir elemana indis kullanarak ulaşma
    println("4. indis : " + cumle(4)) // Sonuç: a

    // for döngüsü ile string elemanları içinde dolaşma
    for(harf <- cumle) println(harf)

    // foreach döngüsü ile string elemanları içinde dolaşma
    cumle.foreach(println)


    // substring
    println(cumle.substring(3,5)) // Sonuç: a

    // İçerik konrolü
    println(cumle.contains("i")) // içeriyorsa true değilse false
    println(cumle.contains("ş")) // içeriyorsa true değilse false


    // String içinde filtreleme
    println(". yı filtrele: " + cumle.filter(_ != '.'))  // tek tırnak olmasına dikkat

    // Zincirleme String operasyonları
    println("İlk 4 elemanı düşür, ilk iki karakteri al, ilk karakteri BÜYÜK yap: " +
    cumle.drop(4).take(2).capitalize)

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


    // stringleri bölme
    println("Split string ile kelimeleri ayırma: ")
    val bolunmusString = cumle.split(" ")
    bolunmusString.foreach(println)

    // , ile ayrılmış stringi bölme
    val virgulString = "süt, kola, ekmek, yumurta, domates"
    val virgulAyrilmisString = virgulString.split(",")
    virgulAyrilmisString.foreach(println)

    // boşlukları kaldırmak için küçük bir ekleme

    val virgulString2 = "süt, kola, ekmek, yumurta, domates"
    val virgulAyrilmisString2 = virgulString.split(",").map(_ .trim)
    virgulAyrilmisString2.foreach(println)


  /********************** Stringler içine değişkenleri yerleştirme *************************/
    val isim = "Erkan"
    val yas = 40

    println(s"$isim, 5 yıl sonra ${yas + 5} yaşında olacak.")


    case class Ogrenci(isim:String, not:Double)
    val ahmet = Ogrenci("Ahmet", 92.44)


    println(s"${ahmet.isim} bu yıl ${ahmet.not} ortalama ile mezun oldu.")
    // Yukarıda süslü parantez kullanmak zorunlu.


  }
}
