import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object sparkdfexample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Myapp").master("local[*]").getOrCreate()
    import spark.implicits._

    val df=Seq(("jhon doe","jhon@example.com",50000),("jane smith","jane@example.com",60000),("bob jhonson","bob@example.com",55000)).toDF("name","email_id","salary")
    df.agg(sum("salary")).show()





  }
}