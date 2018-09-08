package ScalaTemel

object Conditionals {
  def main(args: Array[String]): Unit = {
    // Koşullu ifadeler: ==, !=, >, <, < >, <=, >=
    // Mantıksal ifadeler: &&, ||, !

    var yas = 18

    /********************  IF İLE BİR DEĞİŞKENE DEĞER ATAMAK  ****************/
    val oyVerebilirMi = if(yas >= 18) "oy verebilir" else "oy veremez"
    println("oyVerebilirMi: " + oyVerebilirMi)



    /********************  IF ELSE ÖRNEĞİ  ****************/
    println()
    var enYuksekhizLimiti = 120.0
    var enDusukHizLimiti = 40.0

    var surucuHizi = 150.0
    var surucu = "Murat"

    if((surucuHizi <= enYuksekhizLimiti) && (surucuHizi >= enDusukHizLimiti)){
      println(s"Sayın $surucu, yasal hız limitlerinde seyahat ediyorsunuz.")
    }else{
      println(s"Sayın $surucu, yasal hız limitleri dışında seyahat ediyorsunuz.")
    }

    /********************  IF ELSE IF ÖRNEĞİ  ****************/





  }
}
