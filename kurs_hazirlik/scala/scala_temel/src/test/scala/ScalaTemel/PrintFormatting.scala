package ScalaTemel

object PrintFormatting {
  def main(args: Array[String]): Unit = {
    // %
    val isim = "Emre"
    val yas = 35
    val boy = 1.87

    // print fonksiyonu içinde string değişken yazdırma
    println(s"Merhaba $isim")

    // ondalıklı ifade virgül sonrası basamak sayısını belirleme
    println(f"Benim adım ${isim} ve ben $boy%.3f'dayım.")

    // print fonksiyonu içinde işlem yapma
    println(f"Beş yıl sonra ${yas + 5} yaşında olacağım.")

    printf("%d", 5)
    println()
    // 5'in soluna belirlenen rakama kadar sıfır ekleme
    printf("%02d",5)
    println()
    // 55 olsaydı hiç eklenemezdi çünkü sınır 2 basamak
    printf("%02d",55)
    println()
    // Şimdi daha fazla sıfır ekleyelim
    printf("%010d",55)
    println()
    printf("%.3f", 5.1)


  }
}
