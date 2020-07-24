package com.xunfang.spark.graphx

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ExperimentalCases {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("ExperimentalCases")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext
    //顶点的数据类型是VD:(String,Int)
    val vertexArray = Array((1L, ("Alice", 28)),(2L, ("Bob", 27)),(3L, ("Charlie", 65)),(4L, ("David", 42)),(5L, ("Ed", 55)),(6L, ("Fran", 50)))

    //边的数据类型ED:Int，最后一个值可以理解为亲密度
    val edgeArray = Array(Edge(2L, 1L, 7),Edge(2L, 4L, 2),Edge(3L, 2L, 4),Edge(3L, 6L, 3),Edge(4L, 1L, 1),Edge(5L, 2L, 2),Edge(5L, 3L, 8),Edge(5L, 6L, 3))

    //构造vertexRDD和edgeRDD
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)

    //构造图Graph[VD,ED]
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)

    //过滤出大于30的顶点
    println("年龄大于30的顶点信息")
    graph.vertices.filter{ case (id,(name,age)) => age > 30}.collect.foreach{case (id,(name,age)) => println(s"$name + $age")}
    //这里attr表示的就是边的值
    println("数值大于5的边")
    graph.edges.filter(e => e.attr > 5).collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))
    //从前面triplet的源码可以看出，它是由边和顶点组成的，所以就可以直接输出顶点之间的关系
    println("每个人之间的亲密度")
    for (triplet <- graph.triplets.collect) {println(s"${triplet.srcAttr._1} 对 ${triplet.dstAttr._1} 有${triplet.attr}份亲密度")}
    println("最大出度的顶点，最大入度的顶点，最大度数的顶点。")
    def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {if (a._2 > b._2) a else b}
    println("最大出度:" + graph.outDegrees.reduce(max) + "最大入度:" + graph.inDegrees.reduce(max) + " 度数最多的顶点:" + graph.degrees.reduce(max))
    println("每个人的年龄加10")
    graph.mapVertices{ case (id, (name, age)) => (id, (name, age+10))}.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))
    println(" 构建顶点年龄大于30的子图")
    val subGraph = graph.subgraph(vpred = (id, vd) => vd._2 >= 30)

    //子图所有顶点
    println("子图所有顶点")
    subGraph.vertices.collect.foreach(v => println(s"${v._2._1} is ${v._2._2}"))

    //子图所有边
    println("子图所有边")
    subGraph.edges.collect.foreach(e => println(s"${e.srcId} to ${e.dstId} att ${e.attr}"))

    println("源顶点id大于目标顶点id的数量")
    println(graph.edges.filter(e => e.srcId > e.dstId).count)
  }
}
