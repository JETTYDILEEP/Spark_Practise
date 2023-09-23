package pack
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Day26 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    
    import spark.implicits._
    /*
    val csvdf = spark.read.format("csv").load("file:///C:/data/datatxns.txt")
    csvdf.show()
    
    val jsondf = spark.read.format("json").load("file:///C:/data/devices.csv")
    jsondf.show()
    
    val orcdf =  spark.read.format("orc").load("file:///C:/data/data.orc")
    orcdf.show()
    
    val pardf = spark.read.format("parquet").load("file:///C:/data/data.parquet")
    pardf.show()
    val jsondf = spark.read.format("json").load("file:///C:/data/devices.csv")
    jsondf.show()
    
    jsondf.createOrReplaceTempView("jdf")
    val finaldf = spark.sql("select * from jdf where lat>40")
    finaldf.show()
    finaldf.write.format("orc").mode("overwrite").save("file:///C:/data/orcdata")
    println("====done====")*/
    
    //Task
    val avrodf = spark.read.format("avro").load("file:///C://data/data.avro")
    avrodf.show()
    }
  
}