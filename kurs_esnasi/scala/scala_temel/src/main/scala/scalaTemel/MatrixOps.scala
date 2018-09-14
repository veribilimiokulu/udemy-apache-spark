package scalaTemel

import breeze.linalg.sum
import org.apache.spark.ml.linalg._

object MatrixOps {
  def main(args: Array[String]): Unit = {

    // Sıfırlar ve birlerden oluşan matris yarat
    val zeroMatrix = Matrices.zeros(5,5)
    //println("zeroMatrix: \n" + zeroMatrix)
    val oneMatrix = Matrices.ones(5,5)
    println("oneMatrix: \n" + oneMatrix)


    // Bir array den matris yarat
    val arrayMatrix = Matrices.dense(3,3,Array(1,2,3,4,5,6,7,8,9)).toDense

    val arrayMatrix2 = Matrices.dense(3,3,Array(3,5,7,9,11,13,15,17,19)).toDense


    // Matrisin devriğini (transpozu) almak
    val arrayMatrix3 = Matrices.dense(3,4,Array(1,1,1,1,2,2,2,0,3,5,4,7))
    val arrayMatrix4 = Matrices.dense(3,4,Array(1,0,1,1,2,8,2,0,0,5,2,7))
    println("Matrisin devriği")
    println(arrayMatrix3+ " \n"+ arrayMatrix3.transpose)


    // Sıfır olmayan kaç değer var
    println("Sıfır olmayan değer sayısı: " + arrayMatrix.numNonzeros)


    // Satır ve sütun sayısını söyle
    println("Satır ve sütun sayısı: " + arrayMatrix.numRows, arrayMatrix.numCols)

    //Matrisin elemanlarının toplamı
    println("Matrisin elemanlarının toplamı: " + sum(arrayMatrix.values))
    println("Matrisin elemanlarının toplamı: " + sum(arrayMatrix.toArray))

    // Ana köşegen vererek kalan değerleri sıfırla doldurmak
    val fromDiagonalMatrix = Matrices.diag(Vectors.dense(Array(1.0,5.0,9.0,11.0,13.0))).toDense
    println("Ana köşegenden matris elde etme: \n"+ fromDiagonalMatrix)


    //Birim matris yaratmak
    val eyeMatrix = Matrices.eye(5).toDense
    println("Birim matris: \n" + eyeMatrix)


    // Matris çarpımı
    val multipliedMatrix = arrayMatrix.multiply(arrayMatrix2)
    println("Matris çarpımı: \n" + multipliedMatrix)

  }
}
