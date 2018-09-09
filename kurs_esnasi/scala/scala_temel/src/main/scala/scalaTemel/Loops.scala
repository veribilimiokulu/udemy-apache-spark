package scalaTemel

object Loops {
  def main(args: Array[String]): Unit = {
    /********************  WHILE DÖNGÜSÜ  ****************/
    println("while döngüsü")
    var i = 0
    while(i < 10){
      println(i)
      i += 1
    }

    /********************  DO WHILE DÖNGÜSÜ  ****************/
    println("do while döngüsü")
    do{
      println(i)
      i -= 1
    } while(i > 0)


    /********************  FOR DÖNGÜSÜ  ****************/
    println("for döngüsü")
    for (i <- 0 to 10 by 2){ // hem 0 hem 10 dahil
      println(i)
    }


    println("for döngüsü kelime örneği son sınır dahil")
    val kelime = "Scala"
    for(i <- 0 until kelime.length){
      println(kelime(i))
    }


    println("for döngüsü ile bir collection içinde ilerlemek")
    println("for döngüsü ile bir List içinde ilerlemek")
    val listem = List(0,"Selam",1, "güle güle",2,3,4,5,6)
    for(i <- listem){
      println(i)
    }

    println("for döngüsü ile bir Array içinde ilerlemek")
    val myArray = Array("Bir","İki","Üç",5)
    for (i <- myArray){
      println(i)
    }

      println("for döngüsü ile belirli kurallar uygulayarak collection oluşturmak")
      val tekNumaralar = for{
        i <- 1 to 30
        if(i % 2) == 1
      }yield i

    tekNumaralar.foreach(println)


    /********************  FOREACH DÖNGÜSÜ  ****************/
    // meyveler listesi içinde dolaşıp her bir elemanı büyük harf yapıp yazdırmak için foreach
    val meyveler = List("Elma","Muz","Nar","Ayva")
    meyveler.foreach(x =>{
      println(x.toUpperCase)
    })

  }
}
