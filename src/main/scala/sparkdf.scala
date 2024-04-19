import org.apache.spark.sql.{SaveMode, SparkSession}
object sparkdf {
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder.appName("spark-dev").master("local[*]").getOrCreate()
    val schema="name String,age Int,city String"
    val df=spark.read
      .format("csv")
      .option("header",true)
      .schema(schema)
      .option("path","C:/Users/Hp/Desktop/data.csv").load()
      df.write
      .format("csv")
      .mode(SaveMode.Ignore)
      .option("path", "C:/Users/Hp/Desktop/output")
      .save()



    scala.io.StdIn.readLine()





  }

}
