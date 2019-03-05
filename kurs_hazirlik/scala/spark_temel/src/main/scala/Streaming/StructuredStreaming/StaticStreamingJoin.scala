package Streaming.StructuredStreaming

/*
Erkan ŞİRİN
Spark Structured Streaming ve csv file stream kullanılarak şema yaratma ve
mesleklere göre aylık gelirleri büyükten küçüğe sıralama
 */
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Logger, Level}


object StaticStreamingJoin {
  def main(args: Array[String]): Unit = {
   // log seviyesi
    Logger.getLogger("org").setLevel(Level.ERROR)

    // SparkSession oluşturma
    val spark = SparkSession.builder()
      .master("local[4]")
      .appName("StaticStreamingJoin")
      .config("spark.driver.memory","2g")
      .config("spark.executor.memory","4g")
      .getOrCreate()

    import spark.implicits._

    // order_items için şema - Stream
    val orderItemsSchema = new StructType()
      .add("orderItemName", StringType)
      .add("orderItemOrderId", IntegerType)
      .add("productId",IntegerType)
      .add("orderItemQuantity",IntegerType)
      .add("orderItemSubTotal",DoubleType)
      .add("orderItemProductPrice",DoubleType)

    //products için şema - Statik
    val productsSchema = new StructType()
      .add("productId", IntegerType)
      .add("productCategoryId", IntegerType)
      .add("productName", StringType)
      .add("productDescription", StringType)
      .add("productPrice", DoubleType)
      .add("productImage", StringType)

    // products için statik df oluşturma
    val productsStaticDF = spark.read.format("csv")
      .option("header","true")
      .option("sep",",")
      .schema(productsSchema)
      .load("C:\\Users\\user\\Documents\\streaming-text-files\\products.csv")


   // Order_items için stream dtaframe oluşturma
    val ordersStreamDF = spark.readStream
      .format("csv")
      .option("header","true")
      .option("sep",",")
      .schema(orderItemsSchema)
      .load("D:\\spark-streaming-test\\order_items")

   val joinedDF = ordersStreamDF.join(productsStaticDF, "productId")

    //val groupByOrderStatus = joinedDF.groupBy("orderStatus").count()

    // Streaming başlasın
    val query = joinedDF.writeStream
      .outputMode("append") //sorguda aggregation varsa complete
      .format("console")
      .start()

    query.awaitTermination()

    /*
    Batch: 0
-------------------------------------------
+---------+-------------+----------------+-----------------+-----------------+---------------------+-----------------+--------------------+------------------+------------+--------------------+
|productId|orderItemName|orderItemOrderId|orderItemQuantity|orderItemSubTotal|orderItemProductPrice|productCategoryId|         productName|productDescription|productPrice|        productImage|
+---------+-------------+----------------+-----------------+-----------------+---------------------+-----------------+--------------------+------------------+------------+--------------------+
|      957|            1|               1|                1|           299.98|               299.98|               43|Diamondback Women...|              null|      299.98|http://images.acm...|
|     1073|            2|               2|                1|           199.99|               199.99|               48|Pelican Sunstream...|              null|      199.99|http://images.acm...|
|      502|            3|               2|                5|            250.0|                 50.0|               24|Nike Men's Dri-FI...|              null|        50.0|http://images.acm...|
|      403|            4|               2|                1|           129.99|               129.99|               18|Nike Men's CJ Eli...|              null|      129.99|http://images.acm...|
|      897|            5|               4|                2|            49.98|                24.99|               40|Team Golf New Eng...|              null|       24.99|http://images.acm...|
|      365|            6|               4|                5|           299.95|                59.99|               17|Perfect Fitness P...|              null|       59.99|http://images.acm...|
|      502|            7|               4|                3|            150.0|                 50.0|               24|Nike Men's Dri-FI...|              null|        50.0|http://images.acm...|
|     1014|            8|               4|                4|           199.92|                49.98|               46|O'Brien Men's Neo...|              null|       49.98|http://images.acm...|
|      957|            9|               5|                1|           299.98|               299.98|               43|Diamondback Women...|              null|      299.98|http://images.acm...|
|      365|           10|               5|                5|           299.95|                59.99|               17|Perfect Fitness P...|              null|       59.99|http://images.acm...|
|     1014|           11|               5|                2|            99.96|                49.98|               46|O'Brien Men's Neo...|              null|       49.98|http://images.acm...|
|      957|           12|               5|                1|           299.98|               299.98|               43|Diamondback Women...|              null|      299.98|http://images.acm...|
|      403|           13|               5|                1|           129.99|               129.99|               18|Nike Men's CJ Eli...|              null|      129.99|http://images.acm...|
|     1073|           14|               7|                1|           199.99|               199.99|               48|Pelican Sunstream...|              null|      199.99|http://images.acm...|
|      957|           15|               7|                1|           299.98|               299.98|               43|Diamondback Women...|              null|      299.98|http://images.acm...|
|      926|           16|               7|                5|            79.95|                15.99|               41|Glove It Imperial...|              null|       15.99|http://images.acm...|
|      365|           17|               8|                3|           179.97|                59.99|               17|Perfect Fitness P...|              null|       59.99|http://images.acm...|
|      365|           18|               8|                5|           299.95|                59.99|               17|Perfect Fitness P...|              null|       59.99|http://images.acm...|
|     1014|           19|               8|                4|           199.92|                49.98|               46|O'Brien Men's Neo...|              null|       49.98|http://images.acm...|
|      502|           20|               8|                1|             50.0|                 50.0|               24|Nike Men's Dri-FI...|              null|        50.0|http://images.acm...|
+---------+-------------+----------------+-----------------+-----------------+---------------------+-----------------+--------------------+------------------+------------+--------------------+
only showing top 20 rows


     */
  }
}
