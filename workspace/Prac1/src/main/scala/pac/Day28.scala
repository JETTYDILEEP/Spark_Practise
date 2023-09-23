package pack
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object Day28 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    // partitions
   /* val usdf = spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
    usdf.show()
    val finaldf = usdf.filter("age>10");
    finaldf.filter("age<10").show()
    finaldf.write.format("csv")
                 .mode("overwrite")
                 .partitionBy("state","county")
                 .save("file:///C:/data/uspart")*/
    // AWS Integration
    
   /* val df =  spark.read.format("csv")
                        .option("header","true")
                         .option("fs.s3a.access.key","AKIA5TH4P6EXOZK3Z6HD")
			          .option("fs.s3a.secret.key","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")
			        .load("s3a://zeyodevbb/datatxns.txt")
			  df.show()*/
    //Task
    
    val usdf =  spark.read.format("csv").option("header","true")
                                        .load("file:///C:/data/usdata.csv")
        usdf.select("first_name","last_name").show()
    
  }
}