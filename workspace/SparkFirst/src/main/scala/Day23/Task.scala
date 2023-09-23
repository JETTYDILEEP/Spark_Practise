package Day23

object Task {
  def main(args:Array[String]):Unit={
    //Task1
  /*  println("==== list ====")
    val liststr= List("Bigdata","spark","hive")
    liststr.foreach(println)
    print("=== after concat ===")
    println
    val mapstr = liststr.map( x => "zeyo," + x )
    mapstr.foreach(println)*/
    
    //Task2
    println("=== before split ===")
    println
    val liststr1 = List(  
		"State->TN~City->Chennai"  ,     
		"State->Gujarat~City->GandhiNagar"
		)
		
		liststr1.foreach(println)
		println
		println("===after split ===")
		println
    val flatmap1= liststr1.flatMap( x => x.split("~"))
    val flatmap2= flatmap1.flatMap( x => x.split("->"))
    flatmap2.foreach(println)
    
  }
  
}