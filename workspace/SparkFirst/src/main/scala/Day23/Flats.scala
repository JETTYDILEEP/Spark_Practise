package pack

object Flats {
  
  def main(args:Array[String]):Unit={
    
    val listr = List(
                       "zeyo~analytics"  , 
                       "bigdata~spark"  , 
                       "hive~sqoop"  
                     )
    listr.foreach(println)
    println("==== Falttening ====")
    var flatdata = listr.flatMap(x=>x.split("~"))
    flatdata.foreach(println)
  }
}