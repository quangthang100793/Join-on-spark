package com.spark4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Exercise3 {
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


        //Tính doanh số theo từng khách hàng

        import spark.implicits._


        val schema1 = new StructType()
          .add("custid",IntegerType,true)
          .add("firstname",StringType,true)
          .add("lastname",StringType,true)
          .add("age",IntegerType,true)
            .add("profession",StringType,true)

        val df1 = spark
          .read
            .option("header","false")
          .schema(schema1)
          .csv("D://Data1//custs.txt")
          .as("df1")

        //df.show()
        //df.printSchema()

        val schema2 = new StructType()
          .add("tranid",IntegerType,true)
          .add("date",DateType,true)
          .add("custid",IntegerType,true)
          .add("sales",DoubleType,true)
          .add("shop",StringType,true)
          .add("product",StringType,true)
          .add("city",StringType,true)
          .add("state",StringType,true)
          .add("payment",StringType,true)


        val df2 = spark
          .read
            .option("header","false")
            .option("dateFormat","mm-dd-yyyy")
          .schema(schema2)
          .csv("D://Data1//txns.txt")

        //df1.show()
        //df1.printSchema()

        // tính doanh số khách hàng theo từng năm

        df2
            .withColumn("year",year($"date"))
          .groupBy("custid","year")
          .agg(sum("sales") as "totalsales")
         .as("df2")
         .join(df1,df1("custid") === df2("custid"))
         .select("year","df2.custid","firstname","lastname","totalsales")
          .sort($"year",$"df2.custid")
         .show()

        spark.close()
      }
    }



