package pack
import org.apache.spark._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Day30 {
  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
  /*  val dtdf = spark
                .read
                .format("csv")
                .option("header","true")
                .load("file:///C:/data/dt.txt")
    dtdf.show() */
    //case
  /*  val finaldf = dtdf.selectExpr(
            "id",
            "split(tdate,'-')[2] as year",
            "amount",
            "category",
            "product",
            "spendby",
            "case when spendby ='cash' then 0 else 1 end as status"
            
    )
    
    finaldf.show()*/
    //withcolumn
  /*  val finaldf = dtdf
                  .withColumn("wdate",expr("split(tdate,'-')[2]"))
                  .withColumnRenamed("tdate","year")
                  .withColumn("status",expr("case when spendby='cash' then 0 else 1 end"))
     
    finaldf.show() */
    // groupBy(), agg(), orderBy()
   /* val finaldf = dtdf 
                  .groupBy(col("category"))
                  .agg(count("category").as("count"))
                  .orderBy(col("count"))
    finaldf.show() */
    
    //Task
    val dtdf = spark
               .read
               .format("csv")
               .option("header","true")
               .load("file:///C:/data/dtdata.csv")
    dtdf.show()
    
    val finaldf = Window
                  .partitionBy("dept")
                  .orderBy(col("salary").desc)
                  
    dtdf.withColumn("rank", dense_rank() over finaldf)
                .filter("rank=2")
                .select("dept","salary")
                .orderBy(col("dept"))
                .show()
    
  }
  
}