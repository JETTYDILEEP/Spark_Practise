package Project

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.udf
import scala.io.Source

object Spark {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
   println("===== api data=====")
   println
   val dataf = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
   val st = dataf.mkString
   //println(st)
   val rdd = sc.parallelize(List(st))
   val apidata = spark.read.json(rdd)
   
   apidata.show()
   
   println
   println("==== Avro data ====")
   
   val avrodata = spark.read
                       .format("avro")
                       .load("file:///C:/data/project/projectsample.avro")
   
   avrodata.show()
   
   //apidata.printSchema()
   
   val expapi = apidata.select(
                                
                               col("nationality"),
                               explode(col("results")).as("results"),
                               col("seed"),
                               col("version")
                                   
                               )
                              
 // expapi.printSchema()
  println
  println("===== numericals removed api data =====")
  
  val apiflat = expapi.select(
      
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
      
                             )
                             
    apiflat.show()
//    apiflat.printSchema()
  val remdata = apiflat.withColumn("username", regexp_replace(col("username"), "[0-9]",""))
  remdata.show()
  
  val joineddf = avrodata.join(broadcast(remdata),
                          Seq("username"),"left")
  
  joineddf.show()
  
  val null_data  = joineddf.filter(joineddf("nationality").isNull)
   
  null_data.show()
  
  println
  println("==== Available data with current date ======")
  val not_null_data = joineddf.filter(col("nationality").isNotNull)
                              .withColumn("current_date",current_date())
  not_null_data.show()     
  
  println
  println
  println("======= Not available data with current date ======")

  val not_available = null_data.na.fill("Not Available")
                               .na.fill(0)
                               .withColumn("current_date",current_date())
  not_available.show()
  
  
     
  }
  
}