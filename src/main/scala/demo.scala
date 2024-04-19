import scala.io.StdIn
object demo {
  def main(args: Array[String]): Unit = {
    val size = StdIn.readInt()

    val arr = new Array[Int](size)
    for (i <- 0 until size) {
      println("enter the element at", i)
      arr(i) = StdIn.readInt()
    }
    var sum=0

    for (i <- 0 until size) {
      sum=sum+arr(i)

    }
//    println("sum of element is",sum)
   val n = arr.length
    val expected_sum = n * (n + 1) / 2
    println("missing no:", expected_sum - sum)
  }

}

