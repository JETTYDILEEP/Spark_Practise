package pack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._


object Day32 {
  
  case class schema(
		txnno:Int,
		txndate:String,
		custno:String,
		amount:String,
		category:String,
		product:String,
		city:String,
		state:String,
		spendby:String
		)

  
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    //Complex data processing
    
  /*  val jsondf = spark
                 .read
                 .format("json")
                 .option("multiline","true")
                 .load("file:///C:/data/cm.json")
    jsondf.show()
    jsondf.printSchema()
    
    val flatdf = jsondf.select(
                             "Technology",
                             "TrainerName",
                             "address.permanent",
                             "address.temporary",
                             "id"
                            )
   flatdf.show()
   flatdf.printSchema()*/
    
    println("=====file 1=====")
		println
	
		val collist = List( 
		                  "txnno" ,
		                  "txndate" ,
		                  "custno" ,
		                  "amount" ,
		                  "category" ,
		                  "product" ,
		                  "city" ,
		                  "state" ,
		                  "spendby"
		                  )


		val file1= sc
		.textFile("file:///C:/data/rev/file1.txt")

		println
		println
		println("===Schema df====")
		println
		println
		println


		val gymdata = file1.filter( x => x.contains("Gymnastics"))

		val mapsplit = gymdata.map( x => x.split(","))

		val schemardd = mapsplit.map( x => schema(x(0).toInt,
				x(1),
				x(2),
				x(3),
				x(4),
				x(5),
				x(6),
				x(7),
				x(8))
				)

		val progym = schemardd.filter( x => x.product.contains("Gymnastics"))

		val schemadf = progym.toDF().select(collist.map(col): _*)

		schemadf.show(5)

		println
		println
		println("===Row df====")
		println
		println
		println


		val file2 = sc.textFile("file:///C:/data/rev/file2.txt")

		val rowmapsplit = file2.map( x => x.split(","))

		val rowrdd = rowmapsplit.map( x => Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

		val schema1 = StructType(Array(
				StructField("txnno",IntegerType,true),
				StructField("txndate",StringType,true),
				StructField("custno",StringType,true),
				StructField("amount", StringType, true),
				StructField("category", StringType, true),
				StructField("product", StringType, true),
				StructField("city", StringType, true),
				StructField("state", StringType, true),
				StructField("spendby", StringType, true)
				))


		val rowdf = spark.createDataFrame(rowrdd, schema1).select(collist.map(col): _*)

		rowdf.show(5)

		
	  println
		println
		println("===csv df====")
		println
		println
		println
		
		
		
		val csvdf = spark
		            .read
		            .format("csv")
		            .option("header","true")
		            .load("file:///C:/data/rev/file3.txt")
		            .select(collist.map(col): _*)
		
		
		csvdf.show(5)

		println
		println
		println("===json df====")
		println
		println
		println
		
		
		val jsondf = spark
		              .read
		              .format("json")
		              .load("file:///C:/data/rev/file4.json")
		              .select(collist.map(col): _*)
		
		jsondf.show(5)
		
		
		
		
		println
		println
		println("===parquet df====")
		println
		println
		println
		
		
		val parquetdf = spark
		              .read
		              .load("file:///C:/data/rev/file5.parquet")
		              .select(collist.map(col): _*)
		
		parquetdf.show(5)

	  println
		println
		println("===xml df====")
		println
		println
		println
		
		
		val xmldf = spark
		              .read
		              .format("xml")
		              .option("rowTag","txndata")
		              .load("file:///C:/data/rev/file6")
		              .select(collist.map(col): _*)
		
		xmldf.show(5)

		
		println
		println
		println("===union df====")
		println
		println
		println
		
		
		val uniondf = schemadf
		              .union(rowdf)
		              .union(csvdf)
		              .union(jsondf)
		              .union(parquetdf)
		              .union(xmldf)
		
		uniondf.show(5)
		

		println
		println
		println("===Proc df====")
		println
		println
		println
		
		
		
		val procdf = uniondf
		              .withColumn("txndate", expr("split(txndate,'-')[2]"))
		              .withColumnRenamed("txndate","year")
		              .withColumn("status", expr("case when spendby='cash' then 1 else 0 end"))
		              .filter(col("txnno")>50000)
		              
		
		
		procdf.show(5)
	
		
		println
		println
		println("===cummulative df====")
		println
		println
		println
		
		
		
		val cummulativedf = procdf
		                  .groupBy("category")
		                  .agg(sum("amount").as("total"))
		
		
		cummulativedf.show()
			
		println
		println
		println("===write df====")
		println
		println
		println
		
		
		
		procdf.write.format("avro")
		      .mode("overwrite")
		      .partitionBy("category","spendby")
		      .save("file:///C:/data/revavrod")
		      
		      
		      
		println
		println
		println("===inner df====")
		println
		println
		println     
		      
		      
		val df1 = spark.read.format("csv").option("header","true")
		          .load("file:///C:/data/join1.csv")
		      
		val df2 = spark.read.format("csv").option("header","true")
		          .load("file:///C:/data/join2.csv")      
		      
		      
		val joindf = df1
		            .join(df2, df1("txnno")===df2("tno"),"inner")
		            .drop(df2("tno"))
		
		joindf.show()
		      
		      
		      
		     	      
		println
		println
		println("===left Anti df====")
		println
		println
		println    
		      
		      
		 val df3 = df2.select("tno")     
		      
		val antijoindf = df1
		            .join(df3, df1("txnno")===df3("tno"),"left_anti")
		            .drop(df3("tno"))
		
		antijoindf.show()
		       
		
		      
		      
		      
		      


  }
}