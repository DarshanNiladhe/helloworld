import scala.io.StdIn
object datastructure {
  def main(args: Array[String]): Unit = {
    println("enter the size:")
    val size = StdIn.readInt()

    val arr = new Array[Int](size)
    for (i <- 0 until size) {
      println("enter the no", i)
      arr(i) = StdIn.readInt()
    }
    var len=size-1
    println("the reverse array element is:")
    while(len>=0){
      println(arr(len))
      len=len-1
    }




  }


}

