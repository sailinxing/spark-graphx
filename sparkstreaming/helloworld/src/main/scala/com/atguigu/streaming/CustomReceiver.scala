package com.atguigu.streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

class CustomReceiver(host:String, port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  // 程序启动的时候调用
  override def onStart(): Unit = {

    val socket = new Socket(host,port)

    var inputText = ""

    val reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))

    inputText = reader.readLine()

    while( !isStopped() && inputText != null){
      // 如果接收到了数据 就保存
      store(inputText)
      inputText = reader.readLine()
    }

    // 重新连接，重新执行Onstart方法
    restart("")
  }

  // 程序停止的时候调用
  override def onStop(): Unit = {

  }

}


object CustomReceiver {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("streaming").setMaster("local[*]")

    // 创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    // 从Socket接收数据
    val lineDStream = ssc.receiverStream(new CustomReceiver("master01", 9999))

    val wordDStream = lineDStream.flatMap(_.split(" "))

    val word2CountDStream = wordDStream.map((_, 1))

    val resultDStream = word2CountDStream.reduceByKey(_ + _)

    resultDStream.print()

    // 启动ssc
    ssc.start()
    ssc.awaitTermination()

  }

}

