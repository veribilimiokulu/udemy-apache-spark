package scalaTemel

object Tuples {
  def main(args: Array[String]): Unit = {
    // tuple oluşturma
    var tupleAhmet = (1001, "Ahmet YAMAN", 86.77)
    println(tupleAhmet)

    // Tuple eleman erişim
    println(tupleAhmet._2)


    printf(
      "%s'ın not ortalaması: %.2f\n",
      tupleAhmet._2,
      tupleAhmet._3
    )

    // tuple eleman ekleme. Immutable olduğundan eklenmez
    //tupleAhmet += "8/A"

    // Tuple yazdırmak
    println(tupleAhmet.toString())

    // tuple değerlerini değişkenlere topluca aktarmak
    val(no, isim, notu) = tupleAhmet


    println(no)


    val(not1, isim1, _) = tupleAhmet

    println(not1 + " - " + isim1)



  }
}
