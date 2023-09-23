package pac


import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source

object apijson {
  def main(args:Array[String]):Unit={
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
     //API data read
  for( m <- 1 to 10){
    println("exec-"+m)
  }
    val html = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val urlstring = html.mkString
    //println(urlstring)
    
   val rdd = sc.parallelize(List(urlstring))
   val df = spark.read.json(rdd)
   df.printSchema()
   val flattendf = df.select(
					col("nationality"),
					explode(col("results")).as("results"),
					col("seed"),
					col("version")
					)
			val finaldf = flattendf.select(
					col("nationality"),
					col("results.user.cell"),
					col("results.user.dob"),
					col("results.user.email"),
					col("results.user.gender"),
					col("results.user.location.*"),
					col("results.user.md5"),
					col("results.user.name.*"),
					col("results.user.password"),
					col("results.user.phone"),
					col("results.user.picture.*"),
					col("results.user.registered"),
					col("results.user.salt"),
					col("results.user.sha1"),
					col("results.user.sha256"),
					col("results.user.username"),
					col("seed"),
					col("version")
					).withColumn("today", current_date)
					
  finaldf.printSchema()
  }
  
}