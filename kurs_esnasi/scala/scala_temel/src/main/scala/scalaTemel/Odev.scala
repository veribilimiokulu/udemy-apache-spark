package scalaTemel

object Odev {
  def main(args: Array[String]): Unit = {
    // Soru-1 3'ün 7'nci kuvveti kaçtır?

    println(scala.math.pow(3,7)) // 2187


    //Soru-2 1264 sayısının karekökünü alınız ve sonucun ondalık kısmını iki basamak olarak gösteriniz.
    println()
    var karekok = scala.math.sqrt(1264)
    printf("%.2f",karekok)


    //Soru-3 133 sayısının 5'e bölümünde kalan kaçtır?
      println()
    println(133 % 5)

    // Soru-4 9,5,15,9,63,7,88,25,5,79,15,15,121,9,7,15 elemanlarından ouşan bir Scala listeniz var.
    // Bu listede tekrarlanan elemanları çıkararak küçükten büyüğe sıraya sokunuz.

    val myList = List(9,5,15,9,63,7,88,25,5,79,15,15,121,9,7,15)

    val mySet = myList.toSet
    println(mySet.toList.sorted)

    // Soru-5 ("Ali",5,(8,9),123, ("Cemal","Fatma"),23) Tuple içinde Fatma'ya erişiniz.
    val myTuple = ("Ali",5,(8,9),123, ("Cemal","Fatma"),23)
    println(myTuple._5._2)

    // Soru-6 0 ile 1 arasında her seferinde aynı rassal değeri alan bir ondalıklı sayı üretiniz.
    val rassalSayiUretici = scala.util.Random
    rassalSayiUretici.setSeed(42)
    println(rassalSayiUretici.nextFloat())

    // Soru-7  "kjlsdlklkdslksdlkdslksd" içinde ksd örüntüsünün kaç kez tekrarlandığını bulunuz.
    var myString = "kjlsdlklkdslksdlkdslksd"
    var oruntu = "ksd"
    var sayac = 0

    while(myString.contains(oruntu)){
      myString = myString.replaceFirst(oruntu, "AAA")
      sayac += 1
    }
    println(s"kjlsdlklkdslksdlkdslksd içinde \n$oruntu tekrarlanma sayısı $sayac'dir.")
  }
}
