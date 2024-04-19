import com.sun.org.apache.xalan.internal.lib.ExsltDatetime.year
import org.apache.spark.sql.catalyst.expressions.CurrentRow.deterministic.||
import org.apache.spark.sql.catalyst.expressions.CurrentRow.resolved.||
import org.apache.spark.sql.catalyst.expressions.RLike
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, coalesce, col, collect_list, collect_set, column, concat, concat_ws, count, date_format, dense_rank, desc, explode, explode_outer, first, greatest, lag, last, lead, lit, max, min, rank, regexp_replace, row_number, sequence, split, sum, to_date, when, window}
import org.apache.spark.sql.{SaveMode, SparkSession, functions}
import org.json4s.scalap.scalasig.ClassFileParser.annotations
object largestnum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("OOMExample").master("local[4]").getOrCreate()
      import spark.implicits._

//        val df = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35)).toDF("name", "age")
//        val filteredDF = df.filter(col("age") > 30)
//        filteredDF.show()
    //    val df = Seq(("Alice",25), ("Bob",30), ("Charlie",35)).toDF("name", "age")
    //val filterDF=df.filter(col("age")>25 && col("name").contains("ar"))
    //    filterDF.show()
    //    val df = List((1,"Darshan","Niladhe"), (2,"Kalpesh","Ghumare"), (3,"Vaibhav","Dalvi")).toDF("id","Firstname", "Lastname")
    //    val filterDF = df.filter(col("Lastname") === "Niladhe")
    //    filterDF.show()
    //    val df = List((1, 5, 100), (2, 15, 200), (3, 10, 50)).toDF("product_id", "quantity", "price")
    //       val filterDF = df.filter(col("quantity")>10)
    //        filterDF.show()
    //    val df = List((25, 45000), ( 45, 85000), (57, 95000),(30,60000)).toDF("age", "income")
    //           val filterDF = df.filter(col("age")>30 && (col("income")>50000) )
    //           filterDF.show()
    //    val df = List((1,"IT", 45000), (2,"Comp", 85000), (3,"EXTC", 95000), (4,"MECH", 60000)).toDF("emp-id","dept","salary")
    //              val filterDF = df.filter(col("salary")>40000 && (col("dept")==="IT") )
    //              filterDF.show()
//      val df = Seq(("Alice, 123 | 456", 25), ("Bob, 789", 30), ("Charlie | 321", 35)).toDF("name", "age")
//      val transformedDF = df.withColumn("name", regexp_replace(col("name"), "\\s|,|\\|", ""))
//        transformedDF.show()

//        val df = Seq(("Alice,123", 25), ("Bob,456", 30), ("Charlie,789", 35)).toDF("name", "age")
//        val transformedDF = df.withColumn("name", functions.split(col("name"), ",").getItem(0).cast("int"))
//          .withColumn("id", functions.split(col("name"), ",").getItem(1).cast("int")).select("name","age","id")
//        transformedDF.show()

//    val df = Seq(("abc",123,Array("p","q","r"))).toDF("name", "id", "col3")
//    val filter_df = df.select(col("name"), col("id"),explode(col("col3") ))
//    filter_df.show()

//    val result_df=Seq(
//                    (1,"War","great3D",8.9),
//                    (2,"Science","Fiction",8.5),
//                     (3,"Irish","Boring",6.2),
//                     (4,"Icesong","Fantacy",8.6),
//                      (5,"Housecard","intresting",9.1)
//    ).toDF("id","movie","Description","rating")
//    val output=result_df.filter((col("id")%2!==0) && (col("Description")!=="Boring")).show()


//    val df=Seq((3,4,5),(8,7,6),(1,6,3))toDF("col1","col2","col3")
//   val max_valuers_for_row=df.withColumn("row_max",greatest(col("col1"),col("col2"),col("col3"))).show()
//

//  val  book_issued=Seq((101,"mark","white Tiger"),(102,"Ria","the fountainhead"),(102,"Ria","the secret history"),(101,"mark","bagwat gita"),(103,"loi","the fountainhead")).toDF("student_id","Student_name","book_issued")
//  book_issued.groupBy("student_id","Student_name").agg(concat_ws(";",collect_list("book_issued")).as("Books")).show(truncate = false)
//IBM Interview Question
//    val duplicate=Seq((1,"abc@gmail.com"),(2,"bcd@gmail.com"),(3,"abc@gmail.com"))toDF("id","email")
//    val output=duplicate.groupBy("email").agg(count("*")as("count"))
//      val result=output.filter(col("count")>1).show()

//    val data=Seq(
//      (101,Array("dosa","biryani","idali")),
//      (102,Array("biryani","mineral water")),
//      (103,Array("rice","mineral water","poha")),
//      (109,Array("idali","biryani","poha")))
//        .toDF("id","food_items")
//    val result=data.withColumn("food_items",explode(col("food_items")))
//    result.groupBy("food_items").agg(count("*").as("count")).show()
//

//    val data1=Seq((1,"a"),(2,"b"),(3,"c"))toDF("id","value")
//    val data2=Seq((4,"d"),(5,"e"))toDF("id","value")
//    val union_df=data1.union(data2).show()
//    val df = Seq(("Alice",Array("badminton","tennis")),("bob",Array("tennis","cricket")),("Julie",Array("cricket","carrom"))).toDF("name", "hobbies")
//    val filter_df = df.select(col("name"), explode(col("hobbies").as("Hobbies")))
//    filter_df.show()


//    val student=Seq((1,"steve"),(2,"david"),(3,"aryan"))toDF("student_id","student_name")
//    val marks=Seq((1,"pyspark",90),(1,"sql",100),(2,"sql",70),(2,"pyspark",60),(3,"sql",30),(3,"pyspark",20))toDF("student_id","subject_name","marks")
//    val df1=marks.groupBy("student_id").agg((sum(col("marks"))).as("total_marks"),count(col("subject_name")).as("total_subject"))
//    val df2=df1.withColumn("percentage",col("total_marks")/col("total_subject"))
//    val df3=df2.withColumn("Result",when(col("percentage")>70,"distintion").when(col("percentage")between(60,69),"firstclass").when(col("percentage")between(50,59),"second").when(col("percentage")between(40,49),"third").otherwise("fail"))
//    student.join(df3,"student_id").select("student_id","student_name","percentage","Result").show()

//    val input=Seq(("rahul","sales",10000),("bob","finance",20000),("ankit","marketing",20000),("jack","reporting",15000)).toDF("name","department","salary")
//    val df1=input.filter(col("salary")<20000)
//    val df2=df1.withColumn("bonus",col("salary")*.1)
//    val df3=df2.withColumn("total_salary",col("salary")+col("bonus"))
//    df3.show()

//    val data=Seq(("P001",5,20.0,"C001"),("P002",3,15.5,"C002"),("P003",10,5.99,"C003"),("P004",2,50.0,"C001"),("P005","eight",12.75,"C002")).toDF("ProductCode","Quntity","unitprice","custId")
//    data.filter(col("Quntity").rlike("^[0-9]*$")).show()

//    val input=Seq((10,"anil",50000,18),(11,"vikas",75000,16),(12,"nisha",40000,18),(13,"nidhi",60000,17),(14,"priya",80000,18),(15,"mohit",45000,18),(16,"rajesh",90000,10),(17,"raman",55000,16),(18,"sam",65000,17))toDF("id","name","sal","mngr_id")
//    input.groupBy("mngr_id").agg(avg("sal").as("average_sal")).show()
////
//    val flight=Seq((1,"flight1","delhi","hyd"),(1,"flight2","hyd","kochi"),(1,"flight3","kochi","manglore"),(2,"flight1","mum","ayodhya"),(2,"flight2","ayodhya","gorakhpur"))toDF("id","flightno","origin","destination")
//flight.groupBy("id").agg(first("origin")as("Origin"),last("destination").as("Destination")).show()

//    val df=Seq((1,"arul","sql"),(1,"arul","spark"),(2,"bhumica","sql"),(2,"bhumica","spark"))toDF("id","name","course")
//    df.groupBy("id","name").agg(concat_ws(",",collect_list("course")).as("Coures")).show()


//   val emp_data=Seq(("siva",1,30000),("ravi",2,40000),("prasad",1,50000),("arun",1,30000),("sai",2,20000))toDF("name","dept_id","salary")
//    val df=Window.partitionBy(col("dept_id")).orderBy(col("salary"),col("name"))
//    val df2=emp_data.withColumn("row_num",row_number().over(df)).withColumn("Count",count("name").over(Window.partitionBy("dept_id")))
//    val df1=df2.groupBy("dept_id").agg(max(when(col("row_num")===1,col("name"))).as("min_sal"),max(when(col("row_num")===col("Count"),col("name"))).as("max_sal")).show()



//    val dept=Seq((1,"IT"),(2,"sales")).toDF("Dept_id","Deptname")
//    val emp=Seq((100,"raj",null,1,"2023-04-01",50000),(200,"venki",100,1,"2023-04-13",4000),(200,"venki",100,1,"2023-04-01",4500),(200,"venki",100,1,"2023-05-14",4020)).toDF("empid","empname","mgrid","Dept_id","sal_date","sal")
//    val df=emp.withColumn("newdate",to_date($"sal_date"))
//    val dfjoined=df.join(dept,"Dept_id")
//    val df1=df.as("a").join(df.as("b"),col("a.mgrid")===col("b.empid"),"left")
//    val df2= df1.groupBy("Deptname","a.empname","b.empname"),year("newdate"),date_format("newdate","MMM").sum("salary")).show()
//


//val df=Seq(("M1","D1",2023),("M1","D2",2023),("M1","D3",2023),("M1","D4",2023),("M1","D3",2022),("M1","D4",2022),("M1","D7",2022),("M1","D8",2022)).toDF("ID","CODE","YEAR")
//    df.groupBy("ID").pivot("YEAR").agg(collect_list(col("CODE"))).show()
//val df=Seq(("a","aa",1),("a","aa",2),("b","bb",5),("b","bb",3),("b","bb",4)).toDF("col1","col2","col3")
//    df.groupBy("col1","col2").agg(collect_list("col3")).show()
// val data=Seq(("2023-01-01","AAPL",150),("2023-01-02","AAPL",155),("2023-01-01","GOOG",2500),("2023-01-02","GOOG",2550),("2023-01-01","MSFT",300),("2023-01-02","MSFT",310)).toDF("date","stock","value")
//    val df1=data.withColumn("Date",to_date($"date"))
//    val df2=df1.groupBy("Date","stock").agg(avg("value").as("Avgvalue"))
//    df2.groupBy("stock").agg(max("Avgvalue")).show()

//    val df = Seq((1,"shubham", Array("ps:psa:ps")), (2,"rakesh", Array("ps:psa")), (3,"rupam", Array("ps"))).toDF("id","name", "value")
//     df.withColumn("Value",explode(split(col("value"),":").getItem(0))).show()

//    val df=Seq((13,15,38),(10,20,15)).toDF("X","Y","Z")
//    df.withColumn("triangle",when((col("X")+col("Y")>col("Z"))&&(col("X")+col("Z")>col("Y"))&&(col("Y")+col("z")>col("X")),"YES").otherwise("no")).show()

//    val empolyee=Seq((1,10000,"HR"),(2,20000,"MATH"),(3,30000,"Engineering"),(4,50000,"Engineering"),(5,60000,"CHEM")).toDF("employee_id","salary","department")
//    val avgsal=empolyee.filter(col("department")==="Engineering").agg(avg(col("salary")).as("avgsalary")).first().getDouble(0)
//    val df=empolyee.filter(col("salary")> avgsal).show()
//    val sampledata=Seq((1,"martinez","U795342jy"),(2,"Rodri","7903280317"),(3,"Mane","sh987122e9")).toDF("id","name","phoneno")
//    sampledata.filter(col("phoneno").rlike("^[0-9]*$")).show()

//    val df=Seq((1,"2024-01-01","Acc001","Debit",100),(2,"2024-01-02","Acc001","Debit",50),(3,"2024-01-03","Acc001","credit",300),(4,"2024-01-01","Acc002","credit",100),(5,"2024-01-04","Acc002","Debit",200)).toDF("srno","date","acc_no","transactiontype","amount")
//    df.groupBy("acc_no").agg(sum(when(col("transactiontype")==="credit",col("amount")).otherwise(0)).as("Credit"),sum(when(col("transactiontype")==="Debit",col("amount")).otherwise(0)).as("Debit")).withColumn("netbalance",col("Credit")-col("Debit")).select("netbalance","acc_no").show()




//val data=Seq((1,"sagar",None),(2,None,34),(None,"jhon",46),(5,"alex",None),(4,"alice",None),(3,None,28),(6,"David",39),(7,"sophia",None),(None,None,32),(9,"olivia",None)).toDF("emp_id","name","age")
//    val result=data.select(count(when(col("emp_id").isNull(),1)).as("Empid"),count(when(col("name").isNull(),1)).as("Name"),count(when(col("age").isNull(),1)).as("Age"))
//    result.show()

//    val data=Seq((1,"Abbot"),(2,"Doris"),(3,"Emerson"),(4,"Green"),(5,"Jeames")).toDF("id","student")
//    val df_lead=data.withColumn("next_value",lead("student",1).over(Window.orderBy("id")))
//      val df_lead_lag=df_lead.withColumn("prev_value",lag("student",1).over(Window.orderBy("id")))
//    val df_answer=df_lead_lag.withColumn("Exchange_seat",when(col("id")%2!==0,coalesce(col("next_value"),col("student"))).otherwise(col("prev_value")))
//    df_answer.select("id","student","Exchange_seat").show()

//    val df=Seq((1,"A","m",2500),(2,"B","f",1500),(3,"C","m",5500),(4,"D","f",500)).toDF("id","name","sex","salary")
//   val result_df=df.withColumn("sex",when(col("sex")==="m","f").otherwise("m"))
//    result_df.show()

//val df=Seq((1,"mike",3),(2,"rob",1),(3,"todd",null),(4,"ben",1),(5,"sam",1)).toDF("EmpId","name","ManagerId")
//    val empDF=df.as("emp")
//    val mgrDf=df.as("mgr")
//  val resultdf=empDF.join(mgrDf,empDF("emp.ManagerId")===mgrDf("mgr.EmpId"),"inner").select(empDF("EmpId"),empDF("name"),mgrDf("name").as("managername"))
//    resultdf.show()

//    val df=Seq((1,"2020-17-01",5000),(2,"2021-21-01",1000),(1,"2022-02-02",8000),(2,"2025-17-01",900)).toDF("id","saldate","salary")
//    val df1=df.withColumn("date",to_date($"saldate")).withColumn("year",date_format("date","yyyy")).withColumn("month",date)

//    val data1=Seq((1,"sagar","CSE","UP",80),(2,"Shivani","IT","MP",86),(3,"Muni","Mech","AP",70)).toDF("ID","name","Department","City","marks")
//    val data2=Seq((4,"Raj","CSE","HP"),(5,"Kunal","Mech","Rajasthan")).toDF("ID","name","Department","City")
//    val df2=data2.withColumn("marks",lit(null))
//    val finaldf=data1.union(df2)
//    finaldf.show()

//    val df=Seq((1,"Gaurav",Array("Pune","Banglore","Hyderabad")),(2,"Risabh",Array("Mumbai","Banglore","Pune")),(3,"Amit",Array("Delhi","Chennai","Kolkata")),(4,"Priya",Array("Hyderabad","Pune","Banglore"),(5,"suresh",Array("Banglore","Pune","Mumbai")))).toDF("Empid","Name","Locations")
//    df.withColumn("location",explode(col("Locations"))).show()
//

//    val data=Seq(("james","A","smith","2018","M",3000),("micheal","rose","jones","2010","M",4000),("robert","K","williams","2010","M",4000),("maria","anne","jones","2005","F",4000)).toDF("fname","mname","lname","dob_year","Gender","salary")
//    data.withColumn("fullname",concat_ws(" ",col("fname"),col("mname"),col("lname"))).select("dob_year","Gender","salary","fullname").show()

//    val df=Seq((1,"2020-17-01",5000)).toDF("id","salarydate","salary")
//   val df1= df.withColumn("date",to_date($"salarydate"))
//    df1.withColumn("year",date_format(col("date"),"yyyy")).show()
//    val df1=Seq((101,"ram",10000,"Bangalore"),(102,"Mohan",75000,"Pune"),(103,"ramesh",35000,"Bangalore"),(104,"Manish",50000,"Bangalore"),(105,"rohan",85000,"Pune"),(106,"rakesh",100000,"Bangalore")).toDF("EmpId","EmpName","Empsalary","EmpCity")
//    val df2=Seq((101,"Developer"),(102,"Developer"),(103,"Manager"),(104,"Consultant"),(105,"Consultant"),(106,"Developer")).toDF("EmpId","designation")
//   val df3= df1.join(df2,"EmpId")
//    val df4=Window.partitionBy(col("EmpCity")).orderBy(col("Empsalary")desc)
//    val df5=df3.withColumn("denserank",dense_rank().over(df4))
//    val df6=df5.filter(col("denserank")===1).select("designation","Empsalary")show()

//    val df1=Seq((101,"ram",10000,105),(102,"Mohan",75000,104),(103,"ramesh",35000,104),(104,"Manish",50000,106),(105,"rohan",85000,106),(106,"rakesh",100000,null)).toDF("EmpId","EmpName","salary","ManagerId")
//    val df2=Seq((101,3),(102,2),(103,2),(104,2),(105,3),(106,1)).toDF("EmpId","DeptId")
//    df1.join(df2,"EmpId").show()
//    val df4=df3.groupBy("DeptId").agg(avg(col("salary")))
//    df4.filter(col("salary")>col("Avgsalary")).show()

//
//val df=Seq(("productA","jan",1000),("productA","feb",1500),("productB","jan",1200),("productB","feb",1800),("productC","jan",2000),("productC","feb",5000)).toDF("product","salesmonth","revenue")
//    df.groupBy("product").pivot("salesmonth").agg(sum("revenue")).show()

//  val df=Seq((1,"Jhon",1000,"01/01/2016"),(1,"Jhon",2000,"02/01/2016"),(1,"Jhon",1000,"03/01/2016"),(1,"Jhon",2000,"04/01/2016"),(1,"Jhon",3000,"05/01/2016"),(1,"Jhon",1000,"06/01/2016")).toDF("id","name","salary","Date")
//    val windowspec=Window.orderBy(col("Date"))
//    val df1=df.withColumn("previousvalue",when(col("salary")<lag("salary",1).over(windowspec),"Down").otherwise("up"))
//    df1.show()
//
//    val df=Seq((1,"alice",1000),(2,"bob",2000),(3,"charlie",1500),(4,"david",3000)).toDF("id","name","salary")
//    val windowspec=Window.partitionBy(col("name"))orderBy(col("id"))
//    val df1=df.withColumn("nextvalue",lag("salary",1).over(windowspec)).withColumn("previousvaule",lead("salary",1).over(windowspec)).orderBy("id")
//    df1.filter(col("salary")>1500).show()


  }
}
