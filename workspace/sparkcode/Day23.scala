package pack

object Day23 {
  def main(args:Array[String]):Unit={
   /* println("==== Raw List ====")
    var lisin = List(1,2,3,4)
    lisin.foreach(println)
    println("==== Mul List ====")
    val mullist = lisin.map(x=>x*2)
    mullist.foreach(println)
    println("=== Add List ===")
    val addlist = lisin.map(x=>x+100)
    addlist.foreach(println)*/
    
    println("=== raw list ===")
    val listr = List("zeyobron","analytics","zeyo")
    listr.foreach(println)
    println("=== String Replace ===")
    //val fil = listr.filter(x=>x.contains("zeyo"))
    //val mapstr = listr.map(x=> x.concat(",bigdata"))
    val repstr = listr.map(x=>x.replace("zeyo","tera"))
    repstr.foreach(println)
    
    
  }
}