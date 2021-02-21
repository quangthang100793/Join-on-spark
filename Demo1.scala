package com.spark4

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Demo1 {
      def main(args: Array[String]): Unit = {
        //Turn off Spark Warning/Inforation Messages
        //Logger.getLogger("org").setLevel(Level.ERROR)
        //Logger.getLogger("akka").setLevel(Level.ERROR)
        import org.apache.log4j._
        // tat bot nhung thong bao du thua, chi chua thong bao loi
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)

      val spark = SparkSession
          .builder
          .master("local[*]")
          .appName("Simple Application")
          .getOrCreate()
        val sc = spark.sparkContext
        val SQLContext = spark.sqlContext

        /*val rdd = sc.parallelize(Range(1, 1000)) // phuong thuc parallelize dung de tao ra danh sach RDD
        println(s"Sum: ${rdd.sum}")
        println(s"Min: ${rdd.min}")
        println(s"Max: ${rdd.max}")
        println(s"Count: ${rdd.count}")*/

        import spark.implicits._

        val emp = Seq(
          (1,"Smith",-1,"2018","10","M",3000),
          (2,"Rose",1,"2010","20","M",4000),
          (3,"Williams",1,"2010","10","M",1000),
          (4,"Jones",2,"2005","10","F",2000),
          (5,"Brown",2,"2010","40","",-1),
          (6,"Brown",2,"2010","50","",-1)
        )
        val empColumns = Seq("emp_id","name","superior_emp_id","year_joined","emp_dept_id","gender","salary")

        val empDF = emp.toDF(empColumns:_*)  // khai bao ten cot cho seq emp


        val dept = Seq(
          ("Finance",10),
          ("Marketing",20),
          ("Sales",30),
          ("IT",40)
        )

        val deptColumns = Seq("dept_name","dept_id")
        val deptDF = dept.toDF(deptColumns:_*)

        //empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"inner").show(false)

        // Tinh tong luong theo gender
        //empDF.groupBy("gender").sum("salary").show()

        // Tinh tong luong theo ten phong
   /*     empDF.join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"inner")
            .groupBy("dept_name")
            .sum("salary")
            .show()*/

        //cach 2

        // phai import import org.apache.spark.sql.functions._ moi dung agg duoc
        // phai dung agg thi moi hieu la ta dat alias cho column salary

        empDF.groupBy("emp_dept_id")
          .agg(sum("salary").as("total")) //
            .join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"inner")
            .select("dept_name","total")
            .show

        // Tinh sum va max theo tung phong

        empDF.groupBy("emp_dept_id")
          .agg(sum("salary").as("total"),
            max("salary").as("max"))
          .join(deptDF,empDF("emp_dept_id") === deptDF("dept_id"),"inner")
          .select("dept_name","total", "max")
          .show

        spark.close()
      }
    }



