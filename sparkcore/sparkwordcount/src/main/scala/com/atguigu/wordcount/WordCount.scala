package com.atguigu.wordcount

import org.apache.spark.{SparkConf, SparkContext}


object WordCount extends App with Serializable {

  //声明配置
  val sparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    /*.setJars(List("C:\\Users\\Administrator\\Desktop\\spark2\\sparkcore\\sparkwordcount\\target\\spark-wordcount-1.0-SNAPSHOT-jar-with-dependencies.jar"))
    .setIfMissing("spark.driver.host", "192.168.56.1")*/


  //创建SparkContext
  val sc = new SparkContext(sparkConf)

  //业务逻辑

  val file = sc.textFile("hdfs://master01:9000/README.txt")

  val words = file.flatMap(_.split(" "))

  val word2count = words.map((_,1))

  val result = word2count.reduceByKey(_+_)

  result.saveAsTextFile("hdfs://master01:9000/abc4")

  //sc.textFile("hdfs://master01:9000/README.txt").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).saveAsTextFile("hdfs://master01:9000/abc5")

  //关闭Spark链接
  sc.stop()
}
