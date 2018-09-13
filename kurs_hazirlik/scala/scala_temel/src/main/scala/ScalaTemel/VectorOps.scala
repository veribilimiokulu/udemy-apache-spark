package ScalaTemel
/*
pom.xml dosyasına aşağıdaki dependicy eklemeyi unutmayın
        <dependency>
            <groupId>org.scalanlp</groupId>
            <artifactId>breeze_2.11</artifactId> <!-- or 2.11 -->
            <version>0.13.2</version>
        </dependency>
 */
import breeze.stats._
import org.apache.spark.ml.linalg._

object VectorOps {
  def main(args: Array[String]): Unit = {


    /******************  VECTORS   **************************/
    // Create zero vector
    val zeroVector = Vectors.dense(Array(0.0,0.0,0.0,0.0,0.0))
    // zeroVector: org.apache.spark.ml.linalg.Vector = [0.0,0.0,0.0,0.0,0.0]

    println(zeroVector)

    // Vectors.zero metoduyla Vector yaratma
    val zeroVector2 = Vectors.zeros(5)
    //zeroVector2: org.apache.spark.ml.linalg.Vector = [0.0,0.0,0.0,0.0,0.0]

    // Reach vector elements by index
    println("zeroVector 0'ıncı indeks: ",zeroVector(0))
    // Double = 0.0

    // zero metoduyla oluşturulan Vector ile denseVector yaratma
    val denseVector = zeroVector.toDense
    //denseVector: org.apache.spark.ml.linalg.DenseVector = [0.0,0.0,0.0,0.0,0.0]

    val denseVector2 = zeroVector2.toDense

    // Bir Array'den doğrudan denseVector oluşturamıyoruz
    //val denseVectorFromArray = DenseVector(Array(1,2,3))
    //println(denseVectorFromArray(0))


    // Vectors'ten ml.linalg.DenseVector oluşturma
    val yasVector = Vectors.dense(Array(37.0,42.0,55.0,28.0)).toDense
    // yasVector: org.apache.spark.ml.linalg.DenseVector = [37.0,42.0,55.0,28.0]

    val yasVector2 = Vectors.dense(35.0,27.0,49.0,23.0).toDense


    // Bir ml.linalg.DenseVector ortalamasını almak: breeze.stats.mean kullanıyoruz.
    // Ayrıca denseVector'ü arraya çeviriyoruz
    println("yasVector ortalaması: " + mean(yasVector.toArray)) // 40.5

    // Bir ml.linalg.DenseVector standart sapmasını almak: breeze.stats.stddev kullanıyoruz.
    // Ayrıca denseVectorü arraya çeviriyoruz
    println("yasVector standart sapma: " + stddev(yasVector.toArray))  // 11.269427669584644


    // iki dense vector korelasyon katsayısı
    println("yasVector2: " + variance(yasVector2.toArray))

    // İki vektör arasındaki mesafe
    println("İki vektör arasındaki mesafe: " + Vectors.sqdist(yasVector, yasVector2))

    def kovaryansHesapla(x:DenseVector, y:DenseVector):Double= {
      var kovaryans:Double = 0.0
      val xMean = mean(x.toArray)
      val yMean = mean(y.toArray)
      var total = 0.0
      val n = x.size.toDouble
      for (i <- 0 until x.size){
        total += ((x(i) - xMean) *(y(i) - yMean) )
      }

      kovaryans = (1.0/n)*total
      kovaryans
    }

    println("Kovaryans: " + kovaryansHesapla(yasVector, yasVector2))


    def korelasyonHesapla(x:DenseVector, y:DenseVector):Double= {

      var kovaryans = kovaryansHesapla(x,y)

      val korelasyon = kovaryans /(stddev(x.toArray) * stddev(y.toArray))

      korelasyon
    }

    // İki vektörün korelasyonu
    println("Korelasyon: " + korelasyonHesapla(yasVector,yasVector2))

  }
}
