package ScalaTemel

object Tuples {
  def main(args: Array[String]): Unit = {

    // tuple oluşturma
    var tupleAhmet = (1001, "Ahmet YAMAN", 86.77)

    // tuple elemanlarına erişim. indis 1'den başlar.
    println(tupleAhmet._1) // Sonuç: 1001

    printf("%s'ın not ortalamas: %.2f\n",
      tupleAhmet._2,
      tupleAhmet._3 )


    // tuple eleman ekleme. Immutable olduğundan eklenmez
    // tupleAhmet += "Mehmet"


    // Tuple yazdırmak
    println(tupleAhmet.toString())


    // tuple değerlerini değişkenlere topluca aktarmak
    val(no, isim, notu) = tupleAhmet
    println(no + " " + isim + " " + notu)

    // aktarım esnasında bazılarını es geçmek

    val(no1, isim1, _) = tupleAhmet
    println(no1 + " " + isim1 )
  }
}
