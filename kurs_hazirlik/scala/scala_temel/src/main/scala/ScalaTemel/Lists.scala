package ScalaTemel

object Lists {
  def main(args: Array[String]): Unit = {
  // Immutable

   //Sadece tam sayılardan oluşan bir liste
    val ciftNumaralar = List(2,4,6,8,10)
    println(ciftNumaralar)

    // Liste elemanına erişim
    println("2 indisli elemana erişim: " + ciftNumaralar(2))

    // Liste elemanını değiştirme
    // ciftNumaralar(2) = 20 // hata verir çünkü immutable

    // Liste uzunluğu
    println("Liste uzunluğu: " + ciftNumaralar.size)

    // En büyük ve en küçük eleman
    println("En büyük: " + ciftNumaralar.max + ", en küçük: " + ciftNumaralar.min)

    // Liste veri türleri karşık da olabilir
    val karisikListe = List(1,2,3.3,5.50,false, "Gelin","Görümce","Elti")
    println(karisikListe)

   // 8'den büyük olanları filtrele

   println("8'den büyük olanları filtrele: " + ciftNumaralar.filter(x => x < 8))
   println("8'den büyük olanları filtrele: " + ciftNumaralar.filter(_ > 8))


   // 5'ten küçük olanlar
   println("5'ten küçük olanları filtrele: " + ciftNumaralar.filter(x => x < 5))
   println("5'ten küçük olanları filtrele: " + ciftNumaralar.filter(_ < 5))


   //5'ten küçük olanlara başka bir operasyon daha yapalım
   println("5'ten küçük olanları 10 ile çarp: " + ciftNumaralar.filter(_ < 5).map(_*10))

    // Liste toplamı
    println("Liste toplamı: " + ciftNumaralar.sum)


    // Liste içinden kesit al
    println("1 ile 4 (hariç) elemanlar: " + ciftNumaralar.slice(1,4)) // son sınır hariç


    // ilk indisten başlayarak n sayısı kadar elemanı düşür. Aslında bir nevi filtreleme.
    println("İlk iki elemanı düşür: " + ciftNumaralar.drop(2))
    println("Orijinal liste: " + ciftNumaralar)

    // En sağdaki elemanları seçme
    println("En sağdaki iki eleman: " + ciftNumaralar.takeRight(2))

  // Liste elemanlarını sıralama
    println("Sıralanmış liste: " + ciftNumaralar.sorted)

    // map ile her bir eleman üzerinde işlem yapma
    println(ciftNumaralar.map(x => x*x))

    // İki listeyi zipleme
    val harfler = List("a","b","a","b","b")
    println(harfler.zip(ciftNumaralar))

val zippedList = harfler.zip(ciftNumaralar)

   // println(zippedList.groupBy("a",1))
  }
}
