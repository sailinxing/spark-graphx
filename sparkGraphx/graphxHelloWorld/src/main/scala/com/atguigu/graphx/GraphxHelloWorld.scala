package com.atguigu.graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphxHelloWorld extends App {

  //创建SparkConf
  val sparkConf = new SparkConf().setAppName("graphx").setMaster("local[*]")

  //创建SparkContext
  val sc = new SparkContext(sparkConf)

  //写业务逻辑
  val vertexRDD: RDD[(VertexId, (String, String))] = sc.makeRDD(Array((3L, ("rxin", "student")),
    (7L, ("jgonzal", "postdoc")),
    (5L, ("fanklin", "professor")),
    (2L, ("istoica", "professor"))))

  val edgesRDD: RDD[Edge[String]] = sc.makeRDD(Array(Edge(5L, 7L, "PI2"), Edge(3L, 7L, "Collaborator"),
    Edge(5L, 3L, "Advisor"),
    Edge(2L, 5L, "Colleague"),
    Edge(5L, 7L, "PI")))

  //根据边和顶点创建图对象
  val graph = Graph(vertexRDD, edgesRDD)

  // RDD的操作
  graph.triplets.collect().foreach { edgeTriplat =>
    println(s"[src:] ${edgeTriplat.srcId}  ${edgeTriplat.srcAttr} [edge:] ${edgeTriplat.attr} [dst:] ${edgeTriplat.dstId} ${edgeTriplat.dstAttr}")
  }
  println()

  //根据边创建图对象，顶点属性设置默认值
  val graph2 = Graph.fromEdges(edgesRDD, "abc")
  graph2.triplets.collect().foreach { edgeTriplat =>
    println(s"[src:] ${edgeTriplat.srcId}  ${edgeTriplat.srcAttr} [edge:] ${edgeTriplat.attr} [dst:] ${edgeTriplat.dstId} ${edgeTriplat.dstAttr}")
  }
  println()


  val rawEdges = edgesRDD.map(item => (item.srcId, item.dstId))

  //根据裸边创建图对象，边属性没有的边，边属性默认为1
  val graph3 = Graph.fromEdgeTuples(rawEdges, ("a", "b"))
  graph3.triplets.collect().foreach { edgeTriplat =>
    println(s"[src:] ${edgeTriplat.srcId}  ${edgeTriplat.srcAttr} [edge:] ${edgeTriplat.attr} [dst:] ${edgeTriplat.dstId} ${edgeTriplat.dstAttr}")
  }


  //关闭Spark
  sc.stop()

}
