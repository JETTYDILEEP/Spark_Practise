package pack

object Task {
  def main(args:Array[String]):Unit={
    println("Zeyobron Started")
    println("====raw list====")
    val lisin = List(1,2,3,4)
    lisin.foreach(println)
    print("===Processed===")
    
    val fillist = lisin.filter(x=>x>2)
    
    fillist.foreach(println)
  }
}