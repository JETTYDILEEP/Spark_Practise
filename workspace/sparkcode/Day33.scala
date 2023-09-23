package pack

import org.apache.spark._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Day33 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
  /*  val jsondf = spark
                   .read
                   .format("json")
                   .option("multiline","true")
                   .load("file:///C:/data/picturem.json")
    jsondf.show()
    jsondf.printSchema()
    
  /*  val flatdf = jsondf.selectExpr(
                                    "id",
                                    "image.height as image_height",
                                    "image.url as image_url",
                                    "image.width as image_width",
                                    "name",
                                    "thumbnail.height as thumbnail_height",
                                    "thumbnail.url as thumbnail_url",
                                    "thumbnail.width as thumbnail_width",
                                    "type"
                                  )*/
    val flatdf = jsondf.withColumn("image.height",expr("image.height as image_height"))
                       .withColumn("image.url",expr("image.url as image_url"))
    flatdf.show()
    flatdf.printSchema()*/
    
    //flat data to complex data
    /*val csvdf = spark
                   .read
                   .format("csv")
                   .option("header","true")
                   .load("file:///C:/data/partdata.csv")
    csvdf.show()
    csvdf.printSchema()
    
    val complexdf = csvdf.select(
                                  col("Technology"),
                                  col("TrainerName"),
                                  col("id"),
                                  struct(
                                      struct(
                                        col("permanent"),
                                        col("temporary"),
                                        col("workloc")
                                        ).as("user")
                                  ).as("address")
                                 )
   //complexdf.write.format("json").mode("overwrite").option("multiline","true").save("file:///C:/data/complexd")
            
   complexdf.show()
   complexdf.printSchema()
   
   val jsondf = spark
                   .read
                   .format("json")
                   .option("multiline","true")
                   .load("file:///C:/data/cm.json")
    jsondf.show()
    jsondf.printSchema()
    
    val flatdf = jsondf.select(
                                "Technology",
                                "TrainerName",
                                "address.*",
                                "id"
                                )
   flatdf.show()
   flatdf.printSchema()*/
   
   //Task1
  /* println("====Task====")
    val reqjson = spark.read
                       .format("json")
                       .option("multiline","true")
                       .load("file:///C:/data/reqapi.json")
    reqjson.show()
    reqjson.printSchema()
    
    println
    println("==== Flat Data ====")
    val flatdf = reqjson.select(
                                "data.*",
                                "page",
                                "per_page",
                                "support.*",
                                "total",
                                "total_pages"
                                )
   flatdf.show()
   flatdf.printSchema()*/
    
   //Use Case
  /*  PROV_PH_DTL	PH_TY_CD
			PROV_PH_DTL	PH_NUM
			PROV_PH_DTL	PH_EXT_NUM
			PROV_PH_DTL	PH_PRIM_IND */
    
   val pdf = spark.read
                  .format("csv")
                  .option("header","true")
                  .option("delimiter","~")
                  .load("file:///C:/data/dataset/PractitionerLatest.txt")
 // pdf.show(false)
                  
  val provdf = pdf.select("PROV_PH_DTL")
  provdf.show(false)      
  val edf = provdf.withColumn("PROV_PH_DTL", substring_index(col("PROV_PH_DTL"), "$",1))
                   .select(
                          split(col("PROV_PH_DTL"),"\\|").getItem(0).as("PH_TY_CD"),
                          split(col("PROV_PH_DTL"),"\\|").getItem(1).as("PH_NUM"),
                          split(col("PROV_PH_DTL"),"\\|").getItem(2).as("PH_EXT_NUM"),
                          split(col("PROV_PH_DTL"),"\\|").getItem(3).as("PH_PRIM_IND")
                          )
                 .show()
                         
                          
 
   
   
  }
  
}