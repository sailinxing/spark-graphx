package com.atguigu.stateful

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StateFulWordCount extends App {

  val sparkConf = new SparkConf().setAppName("stateFul").setMaster("local[*]")

  val ssc = new StreamingContext(sparkConf,Seconds(5))
  ssc.sparkContext.setCheckpointDir("./checkpoint")

  val line = ssc.socketTextStream("master01",9999)
  val words = line.flatMap(_.split(" "))
  val word2Count = words.map((_,1))

  /*val result = word2Count.reduceByKey(_+_)
  result.updateStateByKey()*/

  // def updateStateByKey[S: ClassTag]( updateFunc: (Seq[V], Option[S]) => Option[S] ): DStream[(K, S)]
  /*val state = word2Count.updateStateByKey[Int]{ (values:Seq[Int], state:Option[Int]) =>
    state match {
      case None => Some(values.sum)
      case Some(pre) => Some(values.sum + pre)
    }
  }*/

  val state = word2Count.reduceByKeyAndWindow((a:Int,b:Int) => a +b, Seconds(15), Seconds(5))

  state.print()

  ssc.start()
  ssc.awaitTermination()

}
