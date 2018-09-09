package scalaTemel

object Conditionals {
  def main(args: Array[String]): Unit = {
    /********************  IF İLE BİR DEĞİŞKENE DEĞER ATAMAK  ****************/
    var yas = 55
    val oyVerebilirMi = if(yas >= 18) "oy verebilir" else "oy veremez"
    println("oyVerebilirMi: " + oyVerebilirMi)



    /********************  IF ELSE ÖRNEĞİ  ****************/
    println()
    val enYuksekhizLimiti = 120.0
    val enDusukHizLimiti = 40.0

    var surucuHizi = 35.0
    var surucu = "Murat"

    if((surucuHizi <= enYuksekhizLimiti) && (surucuHizi >= enDusukHizLimiti)){

      println(s"Sayın $surucu, yasal hız limitlerinde seyahat ediyorsunuz.")

    }else{

      println(s"Sayın $surucu, yasal hız limitleri dışında seyahat ediyorsunuz.")

    }



    /********************  IF ELSE IF ÖRNEĞİ  ****************/
    println()
    if(yas < 12){

      println("Çocuk")

    }else if((yas >= 12) && (yas < 16)){

      println("Ergen")

    }else if((yas >= 16) && (yas < 22)){

      println("Delikanlı")

    }else if((yas >=22) && (yas < 33)){

      println("Genç")

    }else if((yas >= 33) && (yas < 50)){

      println("Ortayaş")

    }else{

      println("Yaşlı")

    }



  }
}
