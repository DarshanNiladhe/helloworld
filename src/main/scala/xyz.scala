object xyz {
  def main(args: Array[String]): Unit = {
        var num = 343
        val backup = num
        var rem = 0
        var sum = 0
        while (num != 0) {
          rem = num % 10
          sum = sum *10 + rem
          num = num /10
        }
        if (backup == sum)
        {
          print("number is Pallintome")
        }
        else {
          print("number is not pallintrome")
        }
      }

//    var a = 10;
//    var b = 20;
//    println("before swapping", a, b)
//    a = a +b;
//    b = a - b;
//    a = a - b;
//    println("after swaping", a, b)

}