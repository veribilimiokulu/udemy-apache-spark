package ScalaTemel

import scala.collection.mutable.ArrayBuffer

object Arrays {
  def main(args:Array[String]):Unit = {

   /**************  ARRAY  ************************/
    /*
    Array özellikleri:
    1. Muteable'dır. İçindekini güncelleriz.
    2. Ancak boyutu değişmez. 20 elemanlı bir Array'a 21. eklemek için
      yeni bir val ile tutmak gerekir.

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

    //for ile Array içini dolduralım
    for(i <- (0 to rakamArray.length-1)){
      rakamArray(i) = i
      println(rakamArray(i))
    }
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




    /**************  ARRAY BUFFER ************************/
    val meyveler = ArrayBuffer[String]()

    // ArrayBuffer'a indis belirterek eleman ekleme
    meyveler.insert(0,"Elma")
    println("meyveler: " + meyveler)

    // Indis belirtmeden sonuna ekleme
    meyveler += "Portakal"
    println("meyveler: " + meyveler)

    // Array ile çoklu eleman ekleme ++'a dikkat
    meyveler ++= Array("Muz","Armut")
    println("meyveler: " + meyveler)

    //insert metodu ile belirli bir indisten itibaren çoklu eleman ekleme
    meyveler.insert(1,"Nar","Üzüm","Kivi","Karpuz")
    println(meyveler)
    println(meyveler(5))


    /**************  ARRAY BUFFER ************************/
      println("yield ile liste biriktirme ve başka bir değişkene atama")

    // meyveler içinde dolaş her meyveyi bğyğk harf yap, biriktir ve meyveleBuyuk değişkenine ata
    val meyvelerBuyuk = for(meyve <- meyveler) yield meyve.toUpperCase
    println("meyvelerBuyuk yazdırma: ")
    meyvelerBuyuk.foreach(println)

  }
}
