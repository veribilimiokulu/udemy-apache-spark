package ScalaTemel

object Odev {
  def main(args: Array[String]): Unit = {
    // Soru-1 3'ün 7'nci kuvveti kaçtır?
    // scala.math kütüphanesini kullanabiliriz
    println(scala.math.pow(3, 7))


    //Soru-2 1264 sayısının karekökünü alınız ve sonucun ondalık kısmını iki basamak olarak gösteriniz.
    val sonuc = scala.math.sqrt(1264)
    println(sonuc)
    printf("%.2f", sonuc)


    //Soru-3 133 sayısının 5'e bölümünde kalan kaçtır?
    println()
    val kalan = 133 % 5
    println(kalan)


    // Soru-4 9,5,15,9,63,7,88,25,5,79,15,15,121,9,7,15 elemanlarından ouşan bir Scala listeniz var.
    // Bu listede tekrarlanan elemanları çıkararak küçükten büyüğe sıraya sokunuz.

    val myList = List(9, 5, 15, 9, 63, 7, 88, 25, 5, 79, 15, 15, 121, 9, 7, 15)
    // Elemanları tekrardan kurtarmak için listeyi Set'e dönüştür
    val myMap = myList.toSet
    // Tekrardan kurtulmuş Set'i geri listeye dönüştür
    val sortedList = myMap.toList
    //Tekil listeyi sırala ve yazdır
    println(sortedList.sorted)


    // Soru-5 ("Ali",5,(8,9),123, ("Cemal","Fatma"),23) Tuple içinde Fatma'ya erişiniz.
    val myTuple = ("Ali", 5, (8, 9), 123, ("Cemal", "Fatma"), 23)
    println(myTuple._5._2)


    // Soru-6 0 ile 1 arasında her seferinde aynı rassal değeri alan bir ondalıklı sayı üretiniz.
    val r = scala.util.Random
    r.setSeed(42)
    println(r.nextFloat())


    // Soru-7  "kjlsdlklkdslksdlkdslksd" içinde ksd örüntüsünün kaç kez tekrarlandığını bulunuz.
      var myString = "kjlsdlklkdslksdlkdslksd"
      var oruntu = "ksd"
      var sayac = 0

    // while döngüsü içinde örüntü test edilir örüntü var olduğu sürece örüntü ile
    // herhangi bir örüntü burada aaa alındı yer değiştirilir ve string güncellenir.
    // Stringi bu sebeple var olarak oluşturduk.
      while (myString.contains(oruntu)){
      myString = myString.replaceFirst(oruntu, "aaa")
      sayac += 1
      }
      println(s"kjlsdlklkdslksdlkdslksd içinde \n$oruntu tekrarlanma sayısı $sayac'dir.")



    // Soru-7  "kjlsdlklkdslksdlkdslksd" içinde ksd örüntüsünün kaç kez tekrarlandığını bulunuz.























  }
}
