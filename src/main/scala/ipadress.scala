import org.apache.spark.SparkContext

object ipadress {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[*]", "Spark_dev")

    val rdd1 = sc.textFile("C:/Users/Hp/Desktop/ip address.txt");

    val rdd2 = rdd1.flatMap(x => x.split(" "));

    val rdd3 = rdd2.map(x => (x, 1))

    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
    val rdd5=rdd4.sortBy(x=>x._2,false)
    val top = rdd5.take(2)
    println("Higest used word")
    top.foreach(println)

    scala.io.StdIn.readLine()

  }

}
