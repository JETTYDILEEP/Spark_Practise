package pack

object Flat {
  
  def main(args:Array[String]):Unit={
    
   /* val listr = List(
                       "zeyo~analytics"  , 
                       "bigdata~spark"  , 
                       "hive~sqoop"  
                     )
    listr.foreach(println)
    println("==== Falttening ====")
    var flatdata = listr.flatMap(x=>x.split("~"))
    flatdata.foreach(println)*/
    
    println("===== raw list ======")
			
			val liststr = List(
			    
					"Amazon-Jeff-America",
					"Microsoft-BillGates-America",
					"TCS-TATA-india",
					"Reliance-Ambani-INDIA"
					
					)
					
			liststr.foreach(println)
			println("=== Filter ===")
			val fildata = liststr.filter(x=> x.toLowerCase.contains("india"))
			fildata.foreach(println)
			println("=== Flatten===")
			val flatdata = fildata.flatMap(x=>x.split("-"))
			flatdata.foreach(println)
			println("=== replace ===")
			var repstr = flatdata.map(x=>x.replace("india","local"))
			repstr.foreach(println)
			println("=== concat ===")
			var constr = repstr.map(x=>x.concat(",Done"))
			constr.foreach(println)
			  
			    
  }
}