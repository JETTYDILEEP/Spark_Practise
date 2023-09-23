package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import scala.io.Source
object Day35 {
  
  def main(args:Array[String]):Unit={
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
   /*  //API data read
  for( m <- 1 to 10){
    println("exec-"+m)
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
  finaldf.write
         .format("csv")
         .partitionBy("today")
         .mode("append")
         .save("file:///C:/data/urldata")
  }
    // Array generationS
  val cmdf = spark.read
                  .format("json")
                  .option("multiline","true")
                  .load("file:///C:/data/complex/cm.json")
  cmdf.show()
  cmdf.printSchema()
  
  val flatdf = cmdf.withColumn("Students",explode(col("Students")))
  
  flatdf.show()
  flatdf.printSchema()
   
  val compdf = flatdf.groupBy("Technology","TrainerName","id")
                      .agg(collect_list("Students").as("Students"))
  
  compdf.show()
  compdf.printSchema()*/
    //Task 
    /*val reqdf = spark.read
                     .format("json")
                     .option("multiline","true")
                     .load("file:///C:/data/complex/reqres.json")
   
//   reqdf.show()
//   reqdf.printSchema()
//   
   val flatdf = reqdf.withColumn("data",explode(col("data")))
                     .select(
                               col("data.*"),
                               col("page"),
                               col("per_page"),
                               col("support.*"),
                               col("total"),
                               col("total_pages")
                             )
                             
   
   flatdf.show()
   flatdf.printSchema()
   
   val finaldf = flatdf.withColumn("support",struct(
       
                                     col("text"),
                                     col("url")))
                                .drop("text","url")
                       .groupBy("page","per_page","total","total_pages","support")
			                   .agg (
			                           collect_list(
			                                  struct(
			                                            col("avatar"),
			                                            col("email"),
			                                            col("first_name"),
			                                            col("id"),
			                                            col("last_name")
                                                           )

			                                        ).as("data"))
			                                
                                     
                       
                                     
   finaldf.show()
   finaldf.printSchema()*/
    //Task3 optional
   val file1 = spark.read
                    .format("csv")
                    .option("header","true")
                    .option("delimiter","~")
                    .load("file:///C:/data/complex/file1.csv")
  
   val file2 = spark.read
                    .format("csv")
                    .option("delimiter","~")
                    .option("header","true")
                    .load("file:///C:/data/complex/file2.csv")
                    
                    
   file1.show()
   file2.show()
                    
   val flatdata1 = file1.withColumn("empman", explode(split(col("empman"), ",")))
   
   flatdata1.show()
   
   val finaldf = flatdata1.join(file2, flatdata1("empman")=== file2("mno"))
                          .drop("empman","mno")
                          .groupBy("empno","empname")
                          .agg( collect_list(
                                            col("mname")
                                            ).as("mname"))
                        .withColumn("mname", concat_ws(",",col("mname")))
                        .orderBy(col("empno"))
   
   finaldf.show()
  }
  
}