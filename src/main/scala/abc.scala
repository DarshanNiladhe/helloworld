import com.google.gson.annotations.Until
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{abs, array, array_sort, avg, bin, count, sort_array}

import scala.collection.BitSet.empty
import scala.collection.BitSet.empty.until
import scala.io.StdIn
import scala.util.control.Breaks.break


object abc {
  def main(args: Array[String]): Unit = {

    //    Reverse of the Array
    //        val arr = Array(1, 2, 3, 4, 6, 5, 7, 10, 8)
    //        var len = arr.length-1
    //            while (len >= 0)
    //            {
    //             println(arr(len))
    //             len = len -1
    //            }

    //    Sum of Array of elements
    //          {
    //    val arr = Array(1, 2, 3, 4, 6, 5, 7, 10, 8)
    //    var len=arr.length
    //            var sum = 0;
    //            for (i <- 0 until len) {
    //              sum = sum + arr(i)
    //
    //
    //            }
    //            println( sum)
    //
    //          }
    // patteran program
    //            var num=1
    //            for(i<-1 to 4)
    //              {
    //                for(j<-1 to i)
    //                  {
    //                    print(num+" ")
    ////                    num=num+1
    //                  }
    //                  num=num+1
    //                  println(" ")
    //              }

    //                var num=5
    //                var a=5
    //                while (a>0) {
    //                  num = a
    //
    ////                  for(i<-1 to < num)
    //
    //                  for (j <-1 to num) {
    //                    print(num+ " ")
    //                    num = num-1
    //                  }
    //                 a=a-1
    //                 println(" ")
    //                }
    //    Reverse of string
    //            var a="MANGO"
    //            var len=a.length()-1;
    //            for(i<-0 to len)
    //              {
    //                println(a.charAt(i))
    //              }
    //              println("reverse of string")
    //            while (len>=0)
    //              {
    //                println(a.charAt(len))
    //                len=len-1
    //              }
    // swapping of two numbers without using third one
    //        var a=10;
    //        var b=20;
    //        a=a^b;
    //        b=a^b;
    //        a=a^b;
    //        print(a,b)
    //    largest or maximum no in an array
//                var arr=Array(10,1,2,4,7,25)
////                var max=0
//                var min=0
//                for(i<-0 until arr.length)
//                  {
//                    if(min < arr(i))
//                    {
////                      max=arr(i)
//                      min=arr(i)
//                    }
//                  }
//                  println(min)

    //    two sum Array
    //            var arr= Array(10,20,30,80,40)
    //            var target=50
    //            for(i<-0 until arr.length)
    //              {
    //                var a=i+1
    //                for(j<-a until arr.length)
    //                  {
    //                    if(arr(i)+arr(j)== target)
    //                      {
    //                        println(arr(i),arr(j))
    //                      }
    //                 }
    //               }
    //Fibinacci series
    //    var a = 0
    //    var b = 1
    //    //            println("Fibonacci Series upto 10:")
    //            var sum=a+b
    //            while(sum<=10) {
    //              print(" " + sum)
    //              a = b
    //              b = sum
    //              sum = a + b
    ////            }
    //
    //          for(i<-2 to 10 )
    //            {
    //              var c=a+b
    //              print(" "+c)
    //              a=b
    //              b=c
    //
    //
    //            }

    //    reverse of words in string
    //        val a = "mango"
    //        val b=a.toCharArray()
    //    //    println(b)
    //        for(i<-b)
    //          {
    //            print(i+" ")
    //          }
    //          var len=b.length()-1
    //          while(len>=0)
    //            {
    //              println(b)
    //              len=len-1
    //            }

    //              val a="mango is so sweet"
    //              var b= a.split(" ")
    //               println(b)

    //         var len=b.length()-1
    //    //    //
    //    //  str1.foreach(s=>print(s+" "))
    //    //    str1.reverse.foreach(s=>print(s+" "))
    //    //    println()
    //    //    for (i<-str1.length-1 until 0 by -1) {
    //    //      print(str(i)+" ")
    ////    for (i <- 0 to len)
    ////             {
    ////                print(" "+a.charAt(i))
    ////            }
    //
    ////    println("reverse of string")
    //        while (len >= 0) {
    //         println(b.charAt(len))
    //          len = len - 1
    //        }


    //    var num=345
    ////      var original_num=num
    //      var sod=0
    //      var rem=0
    //      while(num!=0)
    //      {
    //        rem=num%10
    //        sod=sod+rem
    //        num=num/10
    //      }
    //      println(sod)
    //      if(original_num==rev)
    //        {
    //          print("number is pallindrone")
    //        }
    //      else
    //        {
    //          print("number not pallintrone")
    //        }


//        var a = "mango  is so sweet"
//        var len = a.length() - 1
//        var original=a
//        var b=""
//        for (i <- 0 to len) {
//          println(a.charAt(i))
//        }
//        println("reverse of string")
//        while (len >= 0) {
//          b=b+a.charAt(len)
//          len = len - 1
//        }
//        print(b)
    //if(original==b)
    //  {
    //    print(" no is string is pallindrome")
    //  }
    //else
    //  {
    //    print()
    //    //  }
    //    var arr = Array(30, 10, 3, 2, 5, 1)
    //    var n = arr.length
    //    for (i <- 0 until n) {
    //      for (j <- 0 until n - i - 1) {
    //        if (arr(j) > arr(j + 1)) {
    //          arr(j) = arr(j) ^ arr(j + 1)
    //          arr(j + 1) = arr(j) ^ arr(j + 1)
    //          arr(j) = arr(j) ^ arr(j + 1)
    //        }
    //        println(arr(j), arr(j + 1))
    //
    //      }

    //
    //    var arr = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    //    val len = arr.length
    //    for (i <- 0 until len) {
    //      println("altrnate even no")
    //      var alteven = 0
    //      while (alteven < len) {
    //        if (arr(alteven) % 2 == 0) {
    //
    //          println(arr(alteven))
    //
    //        }
    //        alteven = alteven + 2
    //      }

  }


  //    for(i<-2 to 100 by 4){
  //      print(" "+i)
  //   / }
  //    var num=370
  //    var sum=0
  //    var temp=num
  //    while(temp>0)
  //      {
  //        var digit=temp%10
  //        sum=sum+(digit*digit*digit)
  //        temp=temp/10
  //      }
  //      if(num==sum)
  //        {
  //          print("yes")
  //        }
  //      else
  //        {
  //          print("no")
  //        }
  //
  //    var num=15
  //    var count=0
  //    for(i<-1 to num)
  //      {
  //        if(num%i==0){
  //          count=count+1
  //        }
  //      }
  //     if(count==2){
  //       println("number is prime")
  //     }
  //    else{
  //       println("number is not prime")
  //     }

  //    val a=Array(1,2,1,4,5,5,6)
  //    val len=a.length
  //    println("the duplicates values from the array is:")
  //    for(i<-0 until len){
  //      for(j<-i+1 until len){
  //        if(a(i)==a(j)){
  //          println(a(j))
  //        }

  //    var a=Array(1,2,3,4,5)
  //    var b=Array(5,6,7,8,9)
  //    var ab=a.length
  //    var ba=b.length
  //    for(i<-0 until ab){
  //      for(j<-0 until ba){
  //        if(a(i)==b(j)){
  //          println("common element between two aray is :"+a(i))
  //        }
  //      }
  //    }
  //    println("Enter the length of array")
  //    var range=scala.io.StdIn.readInt()
  //    var b=new Array[Int](range)
  //    for(k<-1 until range)
  //      {
  //        println("Enter rangee")
  //        b(k)=StdIn.readInt()
  //      }
  //      println("numbers in the given range dvisible by 3 and 11given range dvisible by 3 and 11")
  //      for(k<-1 until range)
  //        {
  //          if(b(k)%3==0 && b(k)%11==0){
  //            println(b(k))
  //          }
  //        }

  //    var sum=0
  //    for(a<-56 to 153){
  //      sum+=a
  //      println("sum of all the numbers from 56 t0 153"+sum)
  //    }

  //    var a = Array(1, 2, 3, 4, 5)
  //    var b = Array(5, 6, 7, 8, 9)
  //    var ab = a.length
  //    var ba = b.length
  //    for (i <- 0 until ab) {
  //      for (j <- 0 until ba) {
  //        if (a(i) == b(j)) {
  //          println("common element between two aray is :" + a(i))
  //        }
  //
  //   var a = Array(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20)
  //   var len=a.length
  //  var k=0
  //  while(k<len){
  //    if(a(k)%2==0){
  //      println(a(k))
  //    }
  //    k=k+2
  //  }
  //}


}

