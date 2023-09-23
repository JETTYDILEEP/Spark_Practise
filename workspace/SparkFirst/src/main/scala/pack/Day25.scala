package pack

import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object Day25 {
  ///case class schema(id:String,tdate:String,Category:String,Product:String)
  def main(args:Array[String]):Unit={
    
    println("===== Started =====")
    println
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    println
    val data = sc.textFile("file:///C:/data/datatxns.txt",1)
    data.foreach(println)
    println
    println("===== Gymnastics ====")
    println
    val gymdata = data.filter(x=>x.contains("Gymnastics"))
    gymdata.foreach(println)
    println
		println("========Column 4th Gymnastics=======")

		println

		val mapsplit = data.map( x => x.split(","))

		//val schemardd = mapsplit.map( x => schema(x(0),x(1),x(2),x(3)))
		val rowrdd = mapsplit.map(x=> Row(x(0),x(1),x(2),x(3)))
		val filterdata = rowrdd.filter(x=> x(3).toString().contains("Gymnastics"))
		
		//val filterrdd = schemardd.filter(  x => x.Product.contains("Gymnastics") )
		//val filterdate = filterrdd.filter(x=>x.tdate.contains("06-26-2011"))
		//filterdate.foreach(println)
		filterdata.foreach(println)
		println
		println("===data frame===")
		//val df = filterrdd.toDF()
    //df.show()
    //df.write.parquet("file:///C:/data/33dataframe")    
    //task
    val structSchema = StructType(Array(
        StructField("id",StringType,true),
        StructField("tdate",StringType,true),
        StructField("category",StringType,true),
        StructField("product",StringType,true)))
    
    val df = spark.createDataFrame(filterdata, structSchema)
    df.show()
    df.createOrReplaceTempView("baahubali")
    val Sql = spark.sql("select * from baahubali where tdate = '06-26-2011'")
    Sql.show()
    
  }
}