package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
object Day34 {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
  /*  val jsondf = spark.read  
                      .format("json")
                      .option("multiline", "true")
                      .load("file:///C:/data/complex/donut.json")
   jsondf.show()
   jsondf.printSchema()
   
   println
   println
   val flatten = jsondf.select(
                                 
                               col("id"),
                               col("image.height").as("image_height"),
                               col("image.url").as("image_url"),
                               col("image.width").as("image_width"),
                               col("name"),
                               col("thumbnail.height").as("thumbnail_height"),
                               col("thumbnail.url").as("thumbnail_url"),
                               col("thumbnail.width").as("thumbnail_width"),
                               col("type")
                               )
    flatten.show()
    flatten.printSchema()
    
   val structdf = flatten.select(
                                 col("id"),
                               struct(                                       
                               col("image_height"),
                               col("image_url"),
                               col("image_width")
                                     ).as("image"),
                               col("name"),
                               struct(  
                               col("thumbnail_height"),
                               col("thumbnail_url"),
                               col("thumbnail_width")
                                     ).as("thumbnail"),
                               col("type")
                                 )
   structdf.show()
   structdf.printSchema()
    
    val petsdf = spark.read
                      .format("json")
                      .option("multiline", "true")
                      .load("file:///C:/data/complex/pets.json")
   petsdf.show()
   petsdf.printSchema()
   
   val flatdf = petsdf.select(
                             col("Address.*"),
                             col("Mobile"),
                             col("Name"),
                             explode(col("Pets")).as("pets"),
                             col("status")
                             )
   flatdf.show()
   flatdf.printSchema()
    
    val reqdf = spark.read
                      .format("json")
                      .option("multiline", "true")
                      .load("file:///C:/data/complex/reqres.json")
   reqdf.show()
   reqdf.printSchema()
   val flatdata = reqdf.select(
                                explode(col("data")).as("data"),
                                col("page"),
                                col("per_page"),
                                col("support.*"), 
                                col("total"),
                                col("total_pages")
                                )
                        .withColumn("avatar",col("data.avatar").as("avatar"))
                        .withColumn("email",col("data.email").as("email"))
                        .withColumn("first_name",col("data.first_name").as("first_name"))
                        .withColumn("id",col("data.id").as("id"))
                        .withColumn("last_name",col("data.last_name").as("last_name"))
                        .drop("data")
  flatdata.show()
  flatdata.printSchema()*/
    
   //Task
    
    val petsdf = spark.read
                      .format("json")
                      .option("multiline","true")
                      .load("file:///C:/data/complex/pets.json")
  
    petsdf.show()
    petsdf.printSchema()
    println
    println
    val flatdf = petsdf.withColumn("pets", explode(col("pets")))
                       .withColumn("Permanent Address",expr("Address.`Permanent address`"))
                       .withColumn("Current Address",expr("Address.`current Address`"))
                       .drop("Address")
    flatdf.show()
    flatdf.printSchema()
                              
   
  }
}