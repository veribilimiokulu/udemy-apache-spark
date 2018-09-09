package scalaTemel

object PrintFormatting {
  def main(args: Array[String]): Unit = {

    println("Merhaba, Dünya!")

    val isim = "Emre"
    val yas = 35
    val boy = 1.87

    // print fonksiyonu içinde string değişken yazdırma
    println(s"Merhaba ${isim}")


    // ondalıklı ifade virgül sonrası basamak sayısını belirleme
    println(f"Benim adım $isim ve ben $boy%.2f'dayım.")


    // print fonksiyonu içinde işlem yapma
    println(f"Beş yıl sonra ${yas + 5} yaşında olacağım.")

    printf("%d", 5)
    println()
    // 5'in soluna belirlenen rakama kadar sıfır ekleme
    printf("%01d",5)
    println()

    printf("%.2f", 5.1)
  }
}
