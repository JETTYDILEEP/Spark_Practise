package Sparkpack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object firstpgm {
  def main(args:Array[String]):Unit={
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
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
  
    //println("helloworld")
    
  }
}