package pack
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object Day24 {
  def main(args:Array[String]):Unit={
   /* val liststr = List("BigData-Spark-Hive",
                    "Spark-Hadoop-Hive")

  liststr.foreach(println)
  println("================flat List============")
  println                
                      

			
  val flatdata = liststr.flatMap( x => x.split("-"))
  flatdata.foreach(println)*/
    println("================Started============")
			println

		val conf = new SparkConf().setAppName("first").setMaster("local[*]")
			val sc = new SparkContext(conf)
			sc.setLogLevel("ERROR")
			println("======== file data=====")

			println("======== file data=====")

			/*val liststr = sc.textFile("file:///C:/data/bdata.txt",1)

			liststr.foreach(println)
			println
			println("================flat List============")
			println                



			val flatdata = liststr.flatMap( x => x.split("-"))
			flatdata.foreach(println) 
			//data write
			val data = sc.textFile("file:///c:/data/usdata.csv")
			data.take(5).foreach(println)
			println
			println("===length data====")
      println
      val lendata = data.filter(x=> x.length()>190)
      lendata.foreach(println)
      println
      println("====flatten data====")
      println
      
      val flatdata = lendata.flatMap(x=> x.split(','))
      flatdata.foreach(println)
      
      println
      println("==== replace data===")
      println
      val replacedata = flatdata.map(x => x.replace("-",""))
      replacedata.foreach(println)
      
      println
      println("==== concat data====")
      println
      val concatdata = replacedata.map(x=> x.concat(",zeyo"))
      concatdata.foreach(println)
      
      println
      println("====Data Write=====")
		
			concatdata.coalesce(1).saveAsTextFile("file:///C:/data/33dir")*/
			println("==== datatxns =====")
			val data = sc.textFile("file:///c:/data/txns")
			data.foreach(println)
			println("=== data contains gymnastics====")
			val filterdata = data.filter(x=> x.contains("Gymnastics"))
			filterdata.foreach(println)
			
  }
}