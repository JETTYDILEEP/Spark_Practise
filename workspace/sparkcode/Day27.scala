package pack
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object Day27 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
   /* val xmldf = spark.read.format("xml").option("rowTag","book")
    .load("file:///C:/data/book.xml")
    
    xmldf.show()
    xmldf.printSchema()
    
    println
    println("==== transition=====")
    val xmltransdf = spark.read.format("xml").option("rowTag","POSLog")
    .load("file:///C:/data/transactions.xml")
    
    xmltransdf.show()
    xmltransdf.printSchema()
    
    val df = spark.read.format("csv").load("file:///C:/data/datatxns.txt")
    
   // df.write.format("csv").save("file:///C:/data/mdata")
    //df.write.format("csv").mode("append").save("file:///C:/data/mdata")
   // df.write.format("csv").mode("overwrite").save("file:///C:/data/mdata")
    df.write.format("csv").mode("ignore").save("file:///C:/data/mdata")*/
    //task
    val df = spark.time(spark.read.format("jdbc")
                                .option("url","jdbc:mysql://zeyodb.czvjr3tbbrsb.ap-south-1.rds.amazonaws.com/zeyodb")
                                .option("user","root")
                                .option("password","Aditya908") 
                                .option("dbtable","kgf")
                                .option("driver","com.mysql.cj.jdbc.Driver")
                                .load())
        df.show()
  }
}