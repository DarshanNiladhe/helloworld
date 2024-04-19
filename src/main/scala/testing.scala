import org.apache.spark.{SparkContext}
import org.apache.log4j.{Logger,Level}
object testing {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)
    val sc = new SparkContext("local[*]", "Spark_dev")

    val rdd1 = sc.textFile("C:/Users/Hp/Desktop/info.txt");

    val rdd2 = rdd1.flatMap(x => x.split(" "));

    val rdd3 = rdd2.map(x => (x, 1))

    val rdd4 = rdd3.reduceByKey((x, y) => x + y)
    val short_rdd = rdd4.sortBy(x => x._2,false)

    val top = short_rdd.take(3)


    short_rdd.collect.foreach(println)
    println("Higest used word")
    top.foreach(println)

    scala.io.StdIn.readLine()


  }

}







