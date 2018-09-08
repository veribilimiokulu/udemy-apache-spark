package ScalaTemel

object Arrays {
  def main(args:Array[String]):Unit = {

    /*
    Array özellikleri:
    1. Mutable'dır. İçindeki güncelleriz.
    2. Ancak boyutu değişmez. 20 elemanlı bir Array'a 21. eklemek için yeni bir val ile tutmak gerekir.

     */
    // 20 elemanlı Int Array oluştur. Hepsine sıfır atar.
    val rakamArray = new Array[Int](20)
    rakamArray(10) = 1502

    // Şimdi atadığımız bu değerin hangi indiste olduğunu bulup yazalım
    var i = 0
    rakamArray.foreach(x =>{
      if(x.equals(1502)){
        println("1502 " + i + "'nci indiste" )
      }else{
        println("1502 burada değil.")
      }
      i += 1
    })

    // String Array oluştur ve bir değerine yeniden atama yap
    val insanlar = Array("Ali","Osman")
    //insanlar(2) = "Mahmut" // Hata verir çünkü böyle bir indis yok

    // 0'ıncı indise yeni bir eleman ata
    insanlar(0) = "Sibel"

    // Ekleniyor anca yeni bir val'de tutlmadığı için yazılırken Mahmut yok
    insanlar ++ Array("Mahmut")
    insanlar.foreach(println)

    // Array'a yeni eleman ekle ve yeni bir val ile tut
    val insanlar2 = insanlar ++ Array("Mahmut")
    insanlar2.foreach(println)

  }
}
