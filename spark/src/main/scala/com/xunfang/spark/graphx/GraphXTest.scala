package com.xunfang.spark.graphx

import org.apache.spark.graphx.PartitionStrategy.RandomVertexCut
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.rdd.RDD

object GraphXTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("GraphXTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val sc = spark.sparkContext

    //创建源顶点数据
    val users: RDD[(VertexId, (String, String))] = sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),(5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))

    //创建边三元组
    val relationships: RDD[Edge[String]] = sc.parallelize(Array(Edge(3L, 7L, "collab"),Edge(5L, 3L, "advisor"),Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))

    val defaultUser = ("John Doe", "Missing")

    // 创建一个图数据
    val graph = Graph(users, relationships,defaultUser)

    //打印边数据
    println("打印边数据")
    println(graph.edges.foreach(println))

    //打印边据
    println("打印边据")
    println(graph.triplets.foreach(println))

    //打印顶点数据
    println("打印顶点数据")
    println(graph.vertices.foreach(println))

    println("获取图中的边数量")
    println(graph.numEdges)
    println("获取图中的顶点数量")
    println(graph.numVertices)
    println("获取图中各顶点的入读数")
    println(graph.inDegrees.foreach(println))
    println("获取图中各顶点的出度数")
    println(graph.outDegrees.foreach(println))
    println("获取图中各顶点的度数")
    println(graph.degrees.foreach(println))
    println("更改前分区数")
    println(graph.edges.getNumPartitions)
    println("更改分区后分区数")
    val graph1 = graph.partitionBy(RandomVertexCut,5)
    println(graph1.edges.getNumPartitions)
    println("给图中每个顶点的occupation的末尾加上'dblab'字符串\"")
    graph.mapVertices{ case (id, (name, occupation)) => (id, (name, occupation+"dblab"))}.vertices.collect.foreach(println)
    println("给图中每个边数据中的属性值设置为源顶点id加一")
    graph.mapEdges(_.srcId + 1).edges.foreach(println)
    println("给图中每个元组的Edge的属性值设置为源顶点属性值加上目标顶点属性值")
    graph.mapTriplets(triplet => triplet.srcAttr._2 + triplet.attr + " " + triplet.dstAttr._2).edges.collect.foreach(println)
    println("返回一个所有边方向取反的新图")
    graph.reverse.edges.foreach(println)
    println("构建顶点属性是prof的子图,并打印子图的顶点")
    val subGraph = graph.subgraph(vpred = (id, attr) => attr._2 == "prof")
    subGraph.vertices.collect.foreach(println)
    println("构造一个子图")
    val validGraph = graph.subgraph(vpred = (id, attr) => attr._2 != "Missing")
    //运行联通分支
    val ccGraph = graph.connectedComponents()
    ccGraph.triplets.foreach(println)
    val validCCGraph = ccGraph.mask(validGraph)
    validCCGraph.triplets.foreach(println)
    println("汇总相邻三元组信息")
    val graph2=GraphGenerators.logNormalGraph(sc, numVertices = 100).mapVertices( (id, _) => id.toDouble )
    val olderFollowers: VertexRDD[(Int, Double)] = graph2.aggregateMessages[(Int, Double)](triplet=>{if (triplet.srcAttr > triplet.dstAttr) {triplet.sendToDst((1, triplet.srcAttr))}},(a, b) => (a._1 + b._1, a._2 + b._2))
    val avgAgeOfOlderFollowers:VertexRDD[Double]=olderFollowers.mapValues((id,value)=>value match{case(count,totalAge)=>totalAge/count})
    avgAgeOfOlderFollowers.collect.foreach(println(_))
  }
}
