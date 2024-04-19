import org.apache.spark.SparkContext


object kmn {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "Spark_dev")
//    val RDD=sc.parallelize(Array(1,2,3,4,5))
//    val filterRdd=RDD.filter(x=>x%2!=0)
//    val count=filterRdd.count()
//    println("count of odd no:"+count)
//val RDD1=sc.parallelize(Array(1,2,3))
//val RDD2=sc.parallelize(Array("A","B"))
//val cartesianRDD=RDD1.cartesian(RDD2)
//cartesianRDD.foreach(println)
//    val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val sum = rdd.reduce(_ + _)
//    val count = rdd.count()
//    val average = sum / count.toDouble
//    println("Average value: " + average)
//     val rdd1 = sc.parallelize(Array((1, "apple"), (2, "banana"), (3,
//     "orange")))
//     val rdd2 = sc.parallelize(Array((1, "red"), (2, "yellow"), (4,
//      "green")))
//    val joinedRdd = rdd1.join(rdd2)
//    joinedRdd.foreach(println)
//val rdd1 = sc.parallelize(Array(1, 2, 3, 4, 5))
//    val rdd2 = sc.parallelize(Array(3, 4, 5))
//    val subtractedRdd = rdd1.subtract(rdd2)
//    subtractedRdd.foreach(println)
//val rdd1 = sc.parallelize(Array(1, 2, 3))
//    val rdd2 = sc.parallelize(Array(3, 4, 5))
//    val unionRdd = rdd1.union(rdd2)
//    unionRdd.foreach(println)
//    val rdd = sc.parallelize(Array(1, 2, 3, 2, 1, 4, 5))
//    val distinctRdd = rdd.distinct()
//    distinctRdd.foreach(println)
//    val rdd = sc.parallelize(Array("apple", "banana", "orange",
//      "pear", "grape"))
//    val searchTerm = "orange"
//    val filteredRdd = rdd.filter(x => x == searchTerm)
//    filteredRdd.foreach(println)
//    val rdd = sc.parallelize(Seq("hello", "world", "hello",
//  "world", "world", "hello", "hi"))
//    val top10 = rdd.map(word => (word, 1))
//      .reduceByKey(_ + _)
//      .sortBy(_._2, false)
//      .take(10)
//      .map(_._1)
//    println("Top 10 words: " + top10.mkString(", "))
//   val rdd = sc.parallelize(Array(1, 2, 3, 4, 5))
//   val doubled_rdd = rdd.map(x=> x * 2)
//    doubled_rdd.collect().foreach(println)



    scala.io.StdIn.readLine()




  }
}