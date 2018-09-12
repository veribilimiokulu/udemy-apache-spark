package scalaTemel

object Lists {
  def main(args: Array[String]): Unit = {
    // Bağlı liste (linked list) ancak immutable


    //Sadece tam sayılardan oluşan bir liste
    val ciftNumaralar = List(2, 4, 6, 8, 10)
    println(ciftNumaralar)


    // Liste elemanına erişim
    println("2 indisli elemana erişim: " + ciftNumaralar(2))


    // Liste elemanını değiştirme
    //ciftNumaralar(2) = 20 // hata verir çünkü immutable

    // Liste uzunluğu
    println("Liste uzunluğu: " + ciftNumaralar.size)


    // En büyük ve en küçük eleman
    println("En büyük: " + ciftNumaralar.max + ", en küçük: " + ciftNumaralar.min)


    // Liste veri türleri karşık da olabilir
    val karisikListe = List(1, 2, 3.3, 5.50, false, "Gelin", "Görümce", "Elti")
    println(karisikListe)
    println(karisikListe(5))

    // 8'den büyük olanları filtrele
    println("8'den büyük olanları filtrele: " + ciftNumaralar.filter(8 <))


    // %'ten küçük olanlar
    println("5'ten küçük olanları filtrele: " + ciftNumaralar.filter(5 >))


    // Liste toplamı
    println("Liste toplamı: " + ciftNumaralar.sum)

    // Liste içinden kesit al
    println("1 ile 4 (hariç) elemanlar: " + ciftNumaralar.slice(1, 4)) // son sınır hariç

    // ilk indisten başlayarak n sayısı kadar elemanı düşür. Aslında bir nevi filtreleme.
    println("İlk iki elemanı düşür: " + ciftNumaralar.drop(2))
    println("Orijinal liste: " + ciftNumaralar)


    // En sağdaki elemanları seçme
    println("En sağdaki iki eleman: " + ciftNumaralar.takeRight(2))

    // Liste elemanlarını sıralama
    println("Sıralanmış liste: " + ciftNumaralar.sorted)

    // map ile her bir eleman üzerinde işlem yapma
    println(ciftNumaralar.map(x => x * x))

    val kareliListe = ciftNumaralar.map(x => x * x)
    println(kareliListe)

    // İki listeyi zipleme
    val harfler = List("a","b","a","b","b")
    println(harfler.zip(ciftNumaralar))

    val zippedList = harfler.zip(ciftNumaralar)
    println(zippedList(0))
  }
}
