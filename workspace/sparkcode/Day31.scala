package pack
import org.apache.spark._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object Day31 {
  def main(args:Array[String]):Unit = {
    
 val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
     /* val df1 = spark
              .read
              .format("csv")
              .option("header","true")
              .load("file:///C:/data/join1.csv")
    df1.show()
    
    val df2 = spark
              .read
              .format("csv")
              .option("header","true")
              .load("file:///C:/data/join2.csv")
    df2.show() */
    //join
 /*   println("===== inner Join =====")
    val injoin = df1.join(df2, df1("txnno") === df2("tno"),"inner")
                    .drop("tno")
    injoin.show()
    
    println("===== left join ==== ")
    
    val lefjoin = df1.join(df2, df1("txnno") === df2("tno"), "left")
                     .drop("tno")
    lefjoin.show()
    
    println("==== right join ====")
    
    val rigjoin = df1.join(df2, df1("txnno") === df2("tno"), "right")
                      .drop("txnno")
     rigjoin.show()
     
    println("==== full join ====")
    
    val fulljoin = df1.join(df2, df1("txnno") === df2("tno"), "full")
                      
    fulljoin.show()
    // real time scene
    /*val listtx= df2.select("tno").rdd.map( x => x.mkString("")).collect().toList
			
			listtx.foreach(println)
			
			
			
			
			val finaldf= df1.filter(  ! (col("txnno").isin(listtx: _*)))
			
			 finaldf.show()*/
    
    println("=========full===================")
			println
			val full = df1.join(df2,df1("txnno")===df2("tno"),"full")
			          .withColumn("txnno",
			              expr("case when txnno is null then tno else txnno end")
			              )
			           .drop("tno")
			           .orderBy(col("txnno"))

			full.show() */
    
    //Task
    /*
    val df3 = df2.select("tno")
    df3.show()
    
    println(" ==== left anti Task 1 ====")
    val left_anti = df1.join(df2,df1("txnno") === df2("tno"), "left_anti")
    left_anti.show()
    println
    println(" === full outer join Task2 ==== ")
    
    val full_outer = df1
                        .join(df2,df1("txnno") === df2("tno"), "full")
                        .withColumn("txnno", 
                            expr( "case when txnno is null then tno else txnno end")
                            )
                        .drop("tno")
                        .orderBy(col("txnno"))
  full_outer.show() */
  
  println("=== optional Task ====")
  
  val uberdf = spark
                    .read
                    .format("csv")
                    .option("header","true")
                    .load("file:///C:/data/uber.csv")
    uberdf.show()
    
    val dday = uberdf
                    .withColumn("date",to_date(col("date"),"mm/dd/yyyy"))
                    .withColumn("date",date_format(col("date"),"E"))
                    .groupBy(col("dispatching_base_number"),col("date"))
                    .agg(sum("trips").as("total_days"))
                    .orderBy(col("dispatching_base_number"))
    dday.show()
                    
                               
    
  }
  
}