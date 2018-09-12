package ScalaTemel

class Insan(x:String, y:Int=0, z:Float = 0.0F) {
  var isim = x
  var yas = y
  var kilo = z

  def kendiniTanit: Unit = {
    println("Adım " + isim + ", yaşım: " + yas + ", kilom: " + kilo)
  }
}



class Point{
  private var _x = 0
  private var _y = 0
  private val bound = 100

  def x = _x
  def x_=(newValue:Int) : Unit = {
    if(newValue < bound ) _x = newValue else printWarning
  }

  def y = _y
  def y_=(newValue: Int):Unit={
    if(newValue < bound) _y = newValue else printWarning
  }

  private def printWarning = println("WARNING: Out of bounds")
}
