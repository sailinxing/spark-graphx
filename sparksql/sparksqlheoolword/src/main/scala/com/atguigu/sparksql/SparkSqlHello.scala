package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSqlHello extends App {

  val sparkConf = new SparkConf().setAppName("sparksql").setMaster("local[*]")

  val spark = SparkSession
    .builder()
    .config(sparkConf)
    .getOrCreate()

  val sc = spark.sparkContext

  val employee = spark.read.json("")

  employee.show()

  employee.select("name").show()

  employee.createOrReplaceTempView("employee")

  spark.sql("select * from employee").show()

  spark.stop()

}
