package com.atguigu.wordcount

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

class CustomerPartitoner(numPartiton:Int) extends Partitioner{

  // 返回分区的总数
  override def numPartitions: Int = {
    numPartiton
  }

  // 根据传入的Key返回分区的索引
  override def getPartition(key: Any): Int = {

    key.toString.toInt % numPartiton

  }
}

object CustomerPartitoner {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("partittoner").setMaster("local[*]")

    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(0 to 10,1).zipWithIndex()

    val r = rdd.mapPartitionsWithIndex( (index,items) => Iterator(index + ":【"+items.mkString(",")+"】") ).collect()

    for (i <- r){
      println(i)
    }

    val rdd2 = rdd.partitionBy(new CustomerPartitoner(5))

    val r1 = rdd2.mapPartitionsWithIndex( (index,items) => Iterator(index + ":【"+items.mkString(",")+"】") ).collect()

    for (i <- r1){
      println(i)
    }

    sc.stop()

  }

}
