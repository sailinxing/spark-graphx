package com.atguigu.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount extends App {

  // 创建配置
  val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")

  // 创建StreamingContext
  val ssc = new StreamingContext(sparkConf, Seconds(5))

  // 从Socket接收数据
  val lineDStream = ssc.socketTextStream("master01", 9999)

  val wordDStream = lineDStream.flatMap(_.split(" "))

  val word2CountDStream = wordDStream.map((_, 1))

  val resultDStream = word2CountDStream.reduceByKey(_ + _)

  resultDStream.print()

  // 启动ssc
  ssc.start()
  ssc.awaitTermination()

}
