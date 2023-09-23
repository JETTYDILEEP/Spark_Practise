package pack
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Day29 {
  def main(args:Array[String]):Unit={
    println("=====Started======")
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    /*  val dtdf = spark
                .read
                .format("csv")
                .option("header","true")
                .load("file:///c:/data/dt.txt")
       
    dtdf.show()
               
    val fdf =  df.filter( 
                           (col("category") === "Exercise"
                           ||
                           col("spendby") === "cash")
                           &&
                           col("id")==="00000002"
                         
                        ).select("category","id")
    
    fdf.show()
    
   println
   println("===== Select =====")
   println
   
   val sdf = dtdf.select("id","tdate")
   sdf.show()
   
   println
   println("==== Filter ====")
   println
   
   val fdf = dtdf.filter(
                       col("category") === "Gymnastics"
                       
                        )
   fdf.show()
   
   println
   println("==== Multi Column And Filter =====")
   println
   
   val adf = dtdf.filter(
                        col("category") === "Gymnastics"
                        &&
                        col("spendby") === "cash"
                         )
   adf.show()
   
   println
			println("=========or operator================")
			println
			val odf = dtdf.filter(

					col("category")==="Gymnastics"
					||
					col("spendby")==="cash"

					)

			odf.show()

			println
			println("=========multi value================")
			println

			val mdf = dtdf.filter(

					col("category") isin ("Gymnastics","Exercise")

					)

			mdf.show()

			println
			println("=========like operator================")
			println

			val cdf = dtdf.filter(

					col("product") like "%Gymnastics%"


					)

			cdf.show()

			println
			println("=========null filter================")
			println
			val ndf = dtdf.filter(

					col("id").isNull 

					)

			ndf.show()

			println
			println("=========not null filter================")
			println
			val nndf = dtdf.filter(

					!  (  col("id").isNull  )

					)

			nndf.show()*/
                
    //Task 1
    
  /*val fdf = dtdf.selectExpr(
          "id",
          "split(tdate,'-')[2] as year",
          "amount",
          "category",
          "product",
          "spendby"
)		
    fdf.show()*/
  
   //Task2
   
    val dtdf = spark
               .read
               .format("csv")
               .option("header","true")
               .load("file:///c:/data/usdata.csv")
    dtdf.show()
    
    val tdf = dtdf.filter( 
                          col("state") === "LA"
                          &&
                          col("age")>10
                          )
   tdf.show()
               
    
    
   
    
    

                       
       
     }
}