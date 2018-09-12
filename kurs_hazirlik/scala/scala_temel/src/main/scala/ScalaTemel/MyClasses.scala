package ScalaTemel


/************** CLASS ÖRNEĞİ-1 **********************/
class Hayvan(var tur:String, var ses:String, var agirlik:Float) {
  this.setTur(tur)

  def getTur() : String = tur
  def getSes() : String = ses
  def getAgirlik () : Float = agirlik

  def setTur(tur:String): Unit ={
    this.tur = tur
  }

  def setSes(ses:String): Unit ={
    this.ses = ses
  }

  override def toString():String={
    return "%s türündeki kuş %s şeklinde ses çıkarır ve yaklaşık %.2f kg. ağırlıktadır.".format(this.tur, this.ses, this.agirlik)

  }

}


/************** CLASS ÖRNEĞİ-2 **********************/
class Point{
  private var _x = 0
  private var _y = 0
  private val sinir = 100

  def x = _x
  def x_=(newValue:Int) : Unit = {
    if(newValue < sinir ) _x = newValue else printWarning
  }

  def y = _y
  def y_=(newValue: Int):Unit={
    if(newValue < sinir) _y = newValue else printWarning
  }

  private def printWarning = println("WARNING: Out of bounds")
}
