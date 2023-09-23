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

object day37 {
  
  def main(args:Array[String]):Unit={
    
   val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
   val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    
    
  }
}