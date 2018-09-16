package ScalaTemel

import scala.util.Random

object PatternMatching {
  def main(args: Array[String]): Unit = {

    // x için 10'a kadar rassal bir sayı üret
    val x: Int = Random.nextInt(4)

    // x'i aşağıdaki case lere match et
    println(
    x match {
      case 0 => "zero"
      case 1 => "one"
      case 2 => "two"
      case _ => "many"
    })


def matchTestStr(x:Int) :String = {
  x match {
    case 0 => "zero"
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }
}

    println(matchTestStr(x))

    def matchTestInt(x:Int) :(Int, Int) = {
      x match {
        case 0 => (0,0*0)
        case 1 => (1,1*1)
        case 2 => (2,2*2)
        case _ => (-1,-1)
      }
    }

    println(matchTestInt(x))






    val ages = List(2,52,44,23,17,14,12,82,51,64)
    val grouped = ages.groupBy(age =>{
      if(age < 18) "child"
      else if(age >= 18 && age < 65) "elder"
      else "senior"
    })
    // ages: List[Int] = List(2, 52, 44, 23, 17, 14, 12, 82, 51, 64)
    // grouped: scala.collection.immutable.Map[String,List[Int]] = Map(senior -> List(82), elder -> List(52, 44, 23, 51, 64), child -> List(2, 17, 14, 12))


  }
}
