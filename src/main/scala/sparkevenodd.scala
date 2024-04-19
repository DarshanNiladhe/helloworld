import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, array_contains, array_join, array_union, avg, broadcast, coalesce, col, collect_list, collect_set, column, concat_ws, count, current_date, date_format, dense_rank, explode, explode_outer, first, lag, last, lit, max, min, monotonically_increasing_id, month, regexp_replace, reverse, row_number, sequence, split, struct, sum, to_date, to_timestamp, translate, when, window, year}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.internal.util.NoPosition.show
object sparkevenodd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()

    import spark.implicits._
    //    val df = List(("john", 25, "Hyd"), ("mohan", 56, "mum"), ("meera", 67, "AP")).toDF("name", "age", "city")
    //
    //    df.select(col("name"), col("age")
    //      , when(col("age") > 20 && col("age") < 30, "ADULT")
    //        .otherwise("OLD").as("category")).show()

    //    val df = List((25,"male"), (45,"female"), (67, "male"),(15,"male")).toDF( "age", "gender")
    //
    //    df.select(col("age"), col("gender")
    //      , when(col("age") >=18 , "True")
    //        .otherwise("flase").as("category")).show()
    //   val df = List((95, "maths"), (60, "civics"), (80, "english"), (85, "marathi")).toDF("score", "subject")
    //
    //       df.select(col("score"), col("subject")
    //        , when(col("score") >=90 , "A").when (col("score") between(70,89),"B").when(col("score")<70,"C")
    //           .otherwise("fail").as("grade")).show()

    //    val df = List(30000,80000,90000,95000,75000).toDF("income")
    //
    //    df.withColumn(("new_income"),when(col("income") > 70000, "High").when(col("income") between(30000, 70000), "Medium").when(col("income") < 30000, "Low")
    //             .otherwise("unknown")).show()
    //val df = List((25, 25000), (60, 10000), (80, 5000), (40, 40000)).toDF("age", "income")
    //
    //          df.select(col("age"), col("income")
    //            , when(col("age") <30 , "young").when (col("age") between(30,60),"adult").when(col("age")>60,"senior")
    //              .otherwise("unknown").as("age_gruop")).show()


    //val scoreData = List(
    //  ("Alice", "Math", 80),
    //  ("Bob", "Math", 90),
    //  ("Alice", "Science", 70),
    //  ("Bob", "Science", 85),
    //  ("Alice", "English", 75),
    //  ("Bob", "English", 95)
    //).toDF("Student", "Subject", "Score")
    //    scoreData.select(col("Student"),col("Subject"),col("Score")).groupBy("Subject","Student").agg(avg("Score")).as("avgscore").show()
    //val ratingData = List(
    //  ("User1", "Movie1", 4.5),
    //  ("User1", "Movie2", 3.5),
    //  ("User1", "Movie3", 2.5),
    //  ("User1", "Movie4", 4.0),
    //  ("User1", "Movie5", 3.0),
    //  ("User1", "Movie6", 4.5),
    //  ("User2", "Movie1", 3.0),
    //  ("User2", "Movie2", 4.0),
    //  ("User2", "Movie3", 4.5),
    //  ("User2", "Movie4", 3.5),
    //  ("User2", "Movie5", 4.0),
    //  ("User2", "Movie6", 3.5)
    //).toDF("User", "Movie", "Rating")
    //    val windowSpec = Window.partitionBy( col("User")).orderBy(col("Movie"))
    //      val avgrating=ratingData.withColumn("Average rating",avg("Rating").over( windowSpec))
    //    avgrating.show()

    //    val salesData = List(
    //      ("Product1", "Category1", 100),
    //      ("Product2", "Category2", 200),
    //      ("Product3", "Category1", 150),
    //      ("Product4", "Category3", 300),
    //      ("Product5", "Category2", 250),
    //      ("Product6", "Category3", 180)
    //
    //    ).toDF("Product", "Category", "Revenue")
    //    val WindowSpec=Window.partitionBy(col("Category")).orderBy(col("Product"))
    //    val runningTotal=salesData.withColumn("RunningTotal",sum("Revenue").over( WindowSpec))
    //    runningTotal.show()

    //    val salesData = List(
    //      ("Product1", "Category1", 100),
    //      ("Product2", "Category2", 200),
    //      ("Product3", "Category1", 150),
    //      ("Product4", "Category3", 300),
    //      ("Product5", "Category2", 250),
    //
    //      ("Product6", "Category3", 180)
    //    ).toDF("Product", "Category", "Revenue")
    //    val windowSpec = Window.partitionBy(col("Product")).orderBy(col("Category"))
    //    val maximumRevenue = salesData.withColumn("Max revenue", max("Revenue").over(windowSpec))
    //    maximumRevenue.show()
    //
    //    val date_df=List(("2023-08-25",null),(null,"2023-09-05"),("2023-10-01",null)).toDF("date1","date2")
    //    date_df.withColumn("new_date",coalesce(col("date1")),lit(-99)).show()
    //val df = List(("2023-10-17", "14:39:00")).toDF("date_str", "time_str")
    //    val formattedDf = df.withColumn("date", to_date($"date_str"))
    //      .withColumn("time", to_timestamp($"time_str"))
    //    formattedDf.show()
    //    val df = List(("2023-10-07", 10), ("2023-10-07", 15), ("2023-11-08", 20)).toDF("date",
    //      "value")
    //    val monthlyAverageDf = df
    //      .withColumn("year_month", date_format($"date", "yyyy-MM"))
    //      .groupBy($"year_month"
    //      .agg(avg($"value").alias("avg_value"))
    //    monthlyAverageDf.show()
    //    spark.stop()
    //val df = List(("2023-10-07", null), (null, "2023-10-08")).toDF("date1", "date2")
    //    val filledDf = df
    //      .withCol umn("date1", coalesce($"date1", lit("2023-01-01")))
    //      .withColumn("date2", when($"date2".isNull, lit("2023-12-31")).otherwise($"date2"))
    //    filledDf.show()
    //    spark.stop()
    //    val df = Seq(("Jhon","Smith"),("Jane","D!oe"),("Bob","Johnson"),("Mike","pant$on"),("tom","L#ee")).toDF("firstname","lastname")
    //    val transformedDF = df.withColumn("Lastname", regexp_replace(col("lastname"), "[!$#]", ""))
    //    transformedDF.show()
    //    val df=List(("Banana",1000,"USA"),("Carrots",1500,"USA"),("Beans",1600,"USA"),
    //    ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"),
    //    ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),
    //    ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")).toDF("Product","Amount","Country")
    //    df.groupBy("Product").pivot("Country").sum("Amount").show()
//         val df_1=Seq((1,"Sagar","UP"),(2,"Shivam","MP"),(3,"Muni","AP")).toDF("ID","Student_name","city")
//         val df_2=Seq((4,"Raj","HP","cse"),(5,"Kunal","Rajasthan","mech")).toDF("ID","Student_name","city","department")
//         val merged_df = df_1.unionByName(df_2,allowMissingColumns=true).show()


    //    val df1=Seq((1,"Alice",25),(2,"Bob",30),(3,"Charlie",28))toDF("id","name","age")
    //    val df2=Seq(("Jhon",22,"Engineer"),("Jane",27,"Marketing"))toDF("name","age","occupation")
    //    val df1adusted=df1.withColumn("occupation",lit(null).cast("String"))
    //    val df2adjusted=df2.withColumn("id",lit(null).cast("int"))
    //    val unionDF=df1adusted.union(df2adjusted)
    //    unionDF.show()


    //    val data=List(("Rudra","math",79),("Rudra","eng",60),("Shivu","math",68),("Shivu","eng",59),("Anu","math",65),("Anu","eng",80))toDF("name","sub","marks")
    //    data.groupBy("name").pivot("sub").agg(sum("marks")).show()

    //  val df=Seq(("nanaware,deepak","~|",36),("patole,sachin","~|",38),("maindad,nitin","~|",34),("abc,cde","~|",23),("xyz,abc","~|",90))toDF("name","delemiter","age")
    //   val split_df=df.withColumn("fname",split(col("name"),",").getItem(1)).withColumn("sir_name",split(col("name"),",").getItem(0)).select("fname","sir_name","age").show()


    //   val df=Seq(("Goa","","AP"),("","AP",null),(null,"","Blgr"))toDF("city1","city2","city3")
    //   val output=df.withColumn("city1",when(col("city1")==="",null).otherwise(col("city1"))).withColumn("city2",when(col("city2")==="",null).otherwise(col("city2"))).withColumn("city3",when(col("city3")==="",null).otherwise(col("city3")))
    //
    //   val df2=output.withColumn("result",coalesce(col("city1"),col("city2"),col("city3"))).select("result").show()
    //
    //        val salesData = Seq(
    //          ("michael", "sales", 4600),
    //          ("robert", "sales", 4100),
    //          ("maria", "finance", 3000),
    //          ("james", "sales", 3000),
    //          ("scott", "finance", 3300),
    //           ("jen","finance",3900),
    //           ("jeff","marketing",3000),
    //           ("kumar","marketing",2000),
    //           ("saif","sales",4100)
    //        ).toDF("emp_name", "department", "salary")
    //        val windowSpec = Window.partitionBy(col("department")).orderBy(col("salary"))
    //        val maximumRevenue = salesData.withColumn("dense_rank",dense_rank().over(windowSpec))
    //         val second_higest_sal=maximumRevenue.filter(col("dense_rank")===2).show()
    //          val Table1 = List(
    //              (1,"michael", "sales"),
    //              (2,"robert","sales"),
    //              (3,"maria", "finance"),
    //              (4,"james", "sales"),
    //              (5,"scott", "finance"),
    //               (6,"jen","finance"),
    //               (7,"jeff","marketing"),
    //               (8,"kumar","marketing"),
    //               (9,"saif","sales")).toDF("id","emp_name","department")
    //          val Table2= Seq((1, 4600),
    //                      (2, 4100),
    //                    (3, 3000),
    //                (4, 3000),
    //                 (5, 3300),
    //                 (6,3900),
    //               (7,3000),
    //               (8,2000),
    //               (9,4100)).toDF("id","salary")
    //    val df_join=Table1.join(Table2,"id")
    //    val df1=Window.partitionBy(col("department")).orderBy(col("salary")desc)
    //    val higest=df_join.withColumn("denserank",dense_rank().over(df1))
    //    val second_higest=higest.filter(col("denserank")===2).show()


    //    val salesData = List(("ProductA",100, 150,200),("ProductB",120,80,110),("ProductC",90,130,180),("ProductD",200,160,120)).toDF("Product", "Jan_sales","Feb_sales","March_sales")
    //    salesData.selectExpr("Product","stack(3,'Jan_sales',Jan_sales,'Feb_sales',Feb_sales,'March_sales',March_sales)as(month,sales)").show()


    //    val inputdata=Seq(("Bags","B100",1000,4000,1500),("Laptops","L100",2000,5000,2500)).toDF("Productdsec","Prodid","amt1","amt2","amt3")
    //    inputdata.selectExpr("Productdsec","Prodid","stack(3,'AMT1',amt1,'AMT2',amt2,'AMT3',amt3)as(name,value)").filter(col("Productdsec")==="Bags").show()
    //     val df=Seq((1))toDF("seq")
    //    val currentdate=df.withColumn("Current_date",current_date().as("current_date"))
    //    currentdate.withColumn("Year",year(col("current_date"))).show()

    //    val df=List(("2024-01-30")).toDF("datestr")
    //    df.withColumn("newdate",to_date($"datestr")).show()

    //    val df = List(("fiction", 2014, 11201), ("fiction", 2014, 12939), ("fiction", 2013, 10436),
    //       ("fiction",2013,9346),("nonfiction",2014,7214),("nonfiction",2014,5800),
    //       ("nonfiction",2013,8922),("nonfiction",2013,7462)).toDF("booktype","salesyear","booksalesmoney")
    //        df.groupBy("booktype").pivot("salesyear").sum("booksalesmoney").show()

    //    val data=Seq(("deelited@youtax.gov"),("anneteak@dotcom.com"),("jimnastik@discotech.com"),("eilavDER@dotcom.com"),("holdmoney@youowetX.GOV"),("DORahjarre@dotcom.com")).toDF("emails")
    //    data.withColumn("name",split(col("emails"),"@").getItem(0)).withColumn("domainname",split(col("emails"),"@").getItem(1)).select("emails","name","domainname")show()

    //    val data=Seq((1,"Gopal",Array("Azure_Databricks","Pyspark")),(2,"Manoj",Array("Java","AWS"))).toDF("id","name","skills")
    //    data.printSchema()
    //    data.select(col("id"), col("name"), explode(col("skills") )).show()

    //    val data = Seq((1, "Gopal","Databricks","Pyspark"), (2, "Manoj","Java","AWS")).toDF("id", "name", "primaryskill","secondaryskill")
    //    data.printSchema()
    //    data.withColumn("hasAzureSkill",array_contains(col("skill"),"Databricks")).show()
    //     val empdata=Seq(("zak",1,10000),("adam",1,10000),("bill",1,70000),("prasad",2,40000),("charles",2,90000),("ram",2,90000)).toDF("empname","dep_id","salary")
    //     val windowSpec=Window.partitionBy("dep_id").orderBy("salary","empname")
    //     val df=empdata.withColumn("rn",row_number().over(windowSpec)).withColumn("Count",count("empname").over(Window.partitionBy("dep_id")))
    //    val df1=df.groupBy("dep_id").agg(max(when(col("rn")===1,col("empname"))).as("minsalary"),max(when(col("rn")===col("Count"),col("empname"))).as("maxsalary")).show()

    //    val df1=Seq((1,"mumbai"),(2,"Banglore"),(3,"Delhi")).toDF("id","name")
    //    val df2=Seq((2,"Banglore"),(1,"mumbai"),(4,"ayodhya")).toDF("id","name")
    //    val diff=df1.subtract(df2).show()

    // val df=Seq(("A","N1","P1"),("A","N2","P2"),("A","N3","P3"),("B","N1","P1"),("C","N1","P1"),("C","N2","P2")).toDF("column_1","column_2","column_3")
    //    df.groupBy("column_1").agg(array_join(reverse(collect_list("column_2")),",").as("Column_2"),array_join(collect_list("column_3"),",").as("Column_3")).sort("column_1").show()
    //    df.groupBy("column_1").agg(array_join(reverse(collect_list("column_2")),",").as("Column_2"),array_join(collect_list("column_3"),",").as("Column_3")).sort("column_1").show()
    //    df.groupBy("column_1").agg(array_join(reverse(collect_list("column_2")),",").as("Column_2"),array_join(collect_list("column_3"),",").as("Column_3")).sort("column_1").show()
    //  df.groupBy("column_1").agg(concat_ws(",",reverse(collect_list("column_2")).as("column_2")),concat_ws(",",collect_list("column_3")).as("column_3")).show()
    //
    //    val df=Seq(("c1","new york","lima"),("c1","london","new york"),("c1","lima","sao paulo"),("c1","sao paulo"),("c2","mumbai","hyderbad"),("c2","surat","pune"),("c2","hyderbad","surat"),("c3","kochi","kurnool"),("c3","lucknow","agra"),("c3","agra","jaipur"),("c3","jaipur","kochi")).toDF("customer","start_location","end_location")
    //    val df_start=df.select(col("customer").as("cust1"),col("start_location"))
    //    val df_end=df.select(col("customer").as("cust2"),col("end_location"))
    //    val df_new=df_start.join(df_end,"df_start.start_location"==="df_end.end_location","outer")
    //    val df_s=df_new.select(col("cust1").as("c1"),col("start_location"),col("cust2").as("c2")).filter(col("cust2").isNull())
    //    val df_e=df_new.select(col("cust2"),col("end_location"),col("cust1")).filter(col("cust1").isNull())
    //
    //    val df_final=df_s.join(df_e,"df_s.c1"==="df_e.cust2","inner").select(col("C1").as("customer"),col("start_location"),col("end_location"))
    //    df_final.show()

    //    val df=Seq(("delhi","M"),("delhi","F"),("delhi","M"),("delhi","M"),("pune","F"),("pune","M"),("pune","F"),("pune","F"),("blr","F"),("blr","F")).toDF("city","gender")
    //    df.groupBy("city").agg(sum(when(col("gender")==="M",1).otherwise(0)).as("Male_count"),sum(when(col("gender")==="F",1).otherwise(0)).as("Female_count"),count("*").as("Total_count")).show()


    //    val df=Seq((1,"Joe",70000,3),(2,"Henry",80000,4),(3,"sam",60000,null),(4,"Max",90000,null)).toDF("id","name","salary","managerid")
    //    df.as("a").join(df.as("b"),col("a.managerid")===col("b.id")).show()


    //    val df=Seq(("IT","jhon"),("IT","jane"),("HR","mike"),("HR","alice")).toDF("department","employeename")
    //    val df1=df.groupBy("department").agg(concat_ws(",",collect_list("employeename")).as("employee")).show()
    //
    //    val df1=Seq(("jhon","ADF"),("jhon","ADB"),("jhon","Powerbi"),("jonne","ADF"),("jonne","SQL"),("jonne","crystal report"),("vikas","ADF"),("vikas","SQL"),("vikas","SSIS")).toDF("empname","skills")
    //    df1.groupBy("empname").agg(concat_ws(",",collect_list("skills")).as("Skills")).show(truncate = false)

    //    val df=Seq((1,"A",20,"31|32|34"),(2,"B",21,"21|32|43"),(3,"C",22,"21|32|11"),(4,"D",23,"10|12|12")).toDF("id","name","age","marks")
    //    df.withColumn("physics",split(col("marks"),"\\|").getItem(0)).withColumn("chemistry",split(col("marks"),"\\|").getItem(1)).withColumn("maths",split(col("marks"),"\\|").getItem(2)).select("id","name","age","physics","chemistry","maths").show()

    //    val df=Seq(("Rahul","Tomato",3),("Virat","Apple",2),("Rahul","Tomato",1),("Rahul","Apple",3),("Priya","Orange",6),("Virat","Apple",1),("Priya","Orange",2)).toDF("name","item","weight")
    //    val df1=df.groupBy("name","item").agg(sum("weight").as("total_weight"))
    //    val finaldf=df1.groupBy("name").agg(collect_set(struct("item","total_weight")).as("items")).show(truncate = false)

    //    val df=Seq((1,3,5,"2019-08-01"),(1,3,6,"2019-08-02"),(2,7,7,"2019-08-01"),(2,7,6,"2019-08-02"),(4,7,1,"2019-07-22"),(3,4,4,"2019-07-21"),(3,4,4,"2019-07-21")).toDF("article_id","author_id","viewer_id","view_date")
    //    val result=df.filter(col("author_id")===col("viewer_id"))
    //    val finaldf=result.groupBy("author_id").agg(count("*")).select("author_id").show()

    //    val df=Seq((1,2022,1,10),(1,2022,2,20),(1,2022,3,30),(1,2023,1,100),(2,2022,1,60)).toDF("id","year","month","sales")
    //    df.groupBy("id","year").agg(max("sales")).show()
    //   val data1=Seq(("1","Rahul","Gwalior"),("2","Gaurav","Pune"),("3","Aakash","Mumbai")).toDF("id","name","city")
    //   val data2=Seq(("4","Amjad","Ahmedabad"),("2","Gaurav","Pune"),("6","Amit","Indore")).toDF("id","name","city")
    ////   val df_All_1=data1.intersect(data2)
    //    val df_All_1=data1.except(data2)
    //    df_All_1.show()
    //    val df=Seq(("Rahul",null,"Rathore"),("Aakash","Kumar","Sharma"),("Vivek","Sharma",null),(null,"Test","Test2")).toDF("fname","middlename","lname")
    //    df.filter(col("fname").isNull()).show()

    //    val df=Seq((1,"Rahul","QA"),(2,"Vivek","Dev"),(3,"Aakash","DEV"),(4,"Gaurav","Support"),(5,"Amit","HR"),(6,"Vinay","Sales")).toDF("id","name","dept")
    //    val df1=Seq((1,500),(2,4000),(3,3000),(4,2000),(5,1000)).toDF("id","sal")
    //    df.join(df1,"Leftanti").show()
    //   val customers=Seq((1,"joe"),(2,"henry"),(3,"sam"),(4,"max")).toDF("id","name")
    //    val orders=Seq((1,3),(2,1)).toDF("id","customerid")
    //    customers.join(orders,customers("id")===orders("customerid"),"left_anti").show()

//     val df = Seq(("C101", "HSBC", 8500), (null, "VISA", ""), (null, "Master", 50000), ("C104", "", 70000)).toDF("id", "account", "SCORE")
//    //    df.filter(col("id").isNull || col("account")==="" || col("SCORE")==="").agg(count("*")).show()
//
    //
    //
    // df.select(sum(when(col("id").isNull || col("id") === "", 1).otherwise(0)).as("ID"), sum(when(col("account").isNull || col("account") === "", 1).otherwise(0)).as("Account"), sum(when(col("SCORE").isNull || col("SCORE") === "", 1).otherwise(0)).as("scroce")).show()
//    val numbers =Seq((1), (2), (3), (4), (5)).toDF("id")
//    numbers.select(avg("id")).show()

//
//    val data=Seq((1,"Tam","Phy"),(2,null,"Che"),(3,null,"Eng"),(4,null,"Math"),(5,null,"Bio"),(6,"sam","History"),(7,null,"Eng"),(8,null,"Psy"),(9,"Tod","Math"),(10,null,"Eng"),(11,null,"Phy"),(12,null,"Hindi")).toDF("row_id","student_name","subject")
//    val df1=Window.orderBy(col("row_id")).rowsBetween(Window.unboundedPreceding,0)
//    val filled_df=data.withColumn("updated_std_name",last("student_name",true).over(df1))
//    val group_df=filled_df.groupBy("updated_std_name").agg(concat_ws(",",collect_list("subject"))).show()
//    val input=Seq(("2024-01-01",1000.5),("2024-01-02",1500.75),("2024-02-03",2000.25),("2024-03-04",1800.0),("2024-02-05",2200.5),("2024-01-03",1000.5),("2024-01-04",1500.75),("2024-02-04",2000.25),("2024-03-05",1800.0),("2024-02-06",2200.5)).toDF("date","revenue")
//    val df1=input.withColumn("new_date",to_date($"date"))
//    val df2=Window.partitionBy(col("new_date")).orderBy(col("new_date")).rowsBetween(Window.unboundedPreceding,Window.currentRow)
//    val df3=df1.withColumn("cumulative_sum",sum("revenue").over(df2))
//    df3.show()

//
//    val data=Seq((1,"mumbai"),(2,"Banglore"),(2,"Banglore"),(3,"Delhi")).toDF("id","name")
//    val data2=Seq((2,"Banglore"),(1,"mumbai"),(4,"chennai")).toDF("id","name")
//    data.join(data2,data("id")===data2("id"),"left_anti").show()
//    val df=Seq((1,"Hello world!"),(2,"Good morning!")).toDF("id","text")
//    df.withColumn("trasialedtext",translate(col("text"),"o","x")).show()
//    val df=Seq((1,"Luke",Array("baseball","soccer")),(2,"lucy",null)).toDF("id","name","like")
//    df.select(col("id"),col("name"),explode_outer(col("like"))).show()

//val df=Seq(("chocolates","5-star"),(null,"dairy milk"),(null,"perk"),(null,"eclair"),("biscuits","britannia"),(null,"goo day"),(null,"boat")).toDF("category","brand_name")
//    val df2=df.withColumn("order_id",monotonically_increasing_id())
//    val df1=Window.orderBy("order_id").rowsBetween(Window.unboundedPreceding,0)
//    val filleddf=df2.withColumn("category_1",last("category",true).over(df1)).select("category_1","brand_name").show()

//    val df=Seq((1,"Data Engineer","SQL"),(2,null,"Python"),(3,null,"AWS"),(4,null,"Snowflake"),(5,null,"Apache Spark"),(6,"Web Developer","Java"),(7,null,"HTML"),(8,null,"CSS"),(9,"Data Scientist","Python"),(10,null,"Machine Learning"),(11,null,"Deep Learning"),(12,null,"Tableau")).toDF("row_id","job_role","skills")
//    val df1=Window.orderBy("row_id").rowsBetween(Window.unboundedPreceding,0)
//    val filleddf=df.withColumn("job_role",last("job_role",true).over(df1)).select("row_id","job_role","skills").show()

//    val visit=Seq((1,23),(2,9),(4,30),(5,54),(6,96),(7,54),(8,54)).toDF("visit_id","customer_id")
//    val transaction=Seq((2,5,310),(3,5,300),(9,5,200),(12,1,910),(13,2,970)).toDF("transaction_id","visit_id","amount")
//    visit.join(transaction,visit("visit_id")===transaction("visit_id"),"left_anti").groupBy("customer_id").agg(count("customer_id").as("count_no_transactions")).show()
//    val df=Seq((1,"2015-01-01",10),(2,"2015-01-02",25),(3,"2015-01-03",20),(4,"2015-01-04",30)).toDF("id","recordDate","Temperature")
//    val df1=df.withColumn("date",to_date($"recordDate"))
//    val df2=Window.orderBy(col("date"))
//    val df3=df1.withColumn("previous_temp",lag("Temperature").over(df2))
//    val df4=df3.filter(col("Temperature")>col("previous_temp")).select("id").show()
//    val df=Seq((1,Array(1,null,3)),(2,Array(null,null,4))).toDF("id","nums")
//    df.withColumn("nums",explode("nums")).filter(col("nums")).isNotNull.show()
//    val df=Seq(("asif","DIV_CHN-56002",95000),("halim","DIV_PUNE-18192",21100)).toDF("name","ref_id","salary")
//    df.withColumn("location",split(col("ref_id"),"_-").getItem(1)).show()
//
//    val transaction=Seq((100,"cosmetic",200),(200,"apparel",250),(300,"shirt",400),(400,"trouser",500),(500,"socks",20),(100,"socks",70),(200,"cosmetic",250),(300,"shoe",400),(400,"socks",25),(500,"shorts",100)).toDF("store_id","item","amount")
//    val store=Seq((100,"store_london"),(200,"store_paris"),(300,"store_frankfurt"),(400,"store_stockholm"),(500,"store_oslo")).toDF("store_id","store_name")
//    transaction.join(broadcast(store),transaction("store_id")===store("store_id")).show()

//    val df=Seq(("alice","travel","2020-02-12","2020-02-20"),("alice","dancing","2020-02-21","2020-02-23"),("alice","travel","2020-02-24","2020-02-28"),("bob","travel","2020-02-11","2020-02-18")).toDF("username","activity","stratdate","endate")
//    val df1=df.withColumn("startdate",to_date($"stratdate")).withColumn("endate",to_date($"endate"))
//    val sepc1=Window.partitionBy(col("username")).orderBy(col("startdate"))
//    val sepc2=Window.partitionBy(col("username")).orderBy(col("username"))
//    val activitydf=df1.withColumn("rank",dense_rank().over(sepc1)).withColumn("count",count("activity").over(sepc2))
//    val finaldf=activitydf.filter(col("rank")===2 || col("count")===1).show()

//    val df=Seq((1001,"jhon joe",50000),(2001,"jane smith",60000),(1003,"michael jhonson",75000),(4000,"emily davis",55000),(1005,"robert brown",70000),(6000,"emma wilson",80000),(1700,"james taylor",65000),(8000,"olivia martinez",72000),(2900,"wiiliam andreson",68000),(3310,"sophia garcia",67000)).toDF("emp_id","name","salary")
//
//  val df1=Window.orderBy(monotonically_increasing_id())
//  val df2=df.withColumn("row_num",row_number().over(df1))
//  df2.filter(col("row_num")%3===0).show()
//    val df=Seq(("2023-10-07","15:30:00")).toDF("date","time")
//    val formatteddf=df.withColumn("formatteddate",date_format($"date","yyyy/MM/dd")).withColumn("formattedtime",date_format($"time","HH:mm:ss"))
//    formatteddf.show()
//val df=Seq((1,101,"air"),(2,101,"train"),(3,102,"air"),(4,103,"train"),(5,104,"air"),(6,104,"train"),(7,105,"train")).toDF("booking_id","customer_id","tickettype")
//val dfmod=df.groupBy("customer_id").agg(sum(when(col("tickettype").isin("air","train"),1)).alias("cnt")).filter(col("cnt")===2).drop(col("cnt"))
//dfmod.show()
//
//val df=Seq(("jhon.doe@gmail.com"),("jane.smith@gmail.com"),("alice.wonder@gmail.com")).toDF("email")
//df.withColumn("firstname",split(col("email"),"\\.").getItem(0)).wi

//    val df=Seq((1,"jhon",30,"sales",50000),(2,"alice",28,"marketing",60000),(3,"bob",32,"finance",55000),(4,"sarah",29,"sales",52000),(5,"mike",31,"finance",58000)).toDF("id","name","age","department","salary")
//  val df1=  df.groupBy("department").agg(avg("salary").alias("averege_salary")).show()
//    df.withColumn("bonus",col("salary")*.1).show()
//   val windowspec=Window.partitionBy(col("department")).orderBy(col("salary")desc)
//  val df1 = df.withColumn("denserank",dense_rank().over(windowspec))
//    df1.filter(col("denserank")===1).show()

//    df.groupBy("department").agg(sum("salary").alias("totol_salary")).orderBy(col("totol_salary")desc).show()
//  val df2 = df.groupBy("department").agg(sum("salary").alias("totol_salary"))
//    val windowspec=Window.orderBy(col("totol_salary")desc)
//    val df1 = df2.withColumn("denserank",dense_rank().over(windowspec))
//    df1.filter(col("denserank")===1).select("department").show()
//    df.filter(col("age")>=30 && col("department")=="sales").show()
//    val df1=Window.partitionBy("department")
//    val df2=df.withColumn("avg_sal",avg("salary").over(df1))
//    df2.withColumn("diff_in_salary",col("salary")-col("avg_sal")).show()





    scala.io.StdIn.readLine()

}
}