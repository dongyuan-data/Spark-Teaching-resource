package com.xunfang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDOperator {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDOperator").setMaster("local[2]")
    val sc = new SparkContext(conf)

    println("map算子演示")
    val rdd = sc.parallelize(1 to 10)
    rdd.collect.foreach(print)
    val rdd1 = rdd.map(_ + 1)
    println
    rdd1.collect.foreach(print)
    println
    println("mapPartitions算子演示")
    val rdd3 = sc.parallelize(0 to 9, 3)
    rdd3.collect.foreach(print)
    val rdd4 = rdd3.mapPartitions(_.map(_*3))
    println
    rdd4.collect.foreach(print)
    println
    println("flatMap算子演示")
    val rdd5 = sc.parallelize(0 to 3)
    rdd5.collect.foreach(print)
    val rdd6 = rdd5.flatMap(x => Array(x,x*x,x*x*x))
    println
    rdd6.collect.foreach(print)
    println
    println("filter算子演示")
    val rdd7 = sc.parallelize(Array("spark-shell","spark-ML","hadoop","hdfs"))
    rdd7.collect.foreach(print)
    val rdd8 = rdd7.filter(_.contains("spark"))
    println
    rdd8.collect.foreach(print)
    println
    println("groupBy算子演示")
    val rdd9 = sc.parallelize(1 to 9)
    rdd9.collect.foreach(print)
    val rdd10 = rdd9.groupBy(_%3)
    println
    rdd10.collect.foreach(print)
    println
    println("distinct算子演示")
    val rdd11 = sc.parallelize(Array(0,1,1,3,2,3,2,3,4,5,6))
    rdd11.collect.foreach(print)
    val rdd12 = rdd11.distinct()
    println
    rdd12.collect.foreach(print)
    println
    println("coalesce算子演示")
    val rdd13 = sc.parallelize(0 to 9 , 3)
    println(rdd13.partitions.size)
    val rdd14 = rdd13.coalesce(2)
    println(rdd14.partitions.size)

    println("repartition算子演示")
    val rdd15 = sc.parallelize(0 to 9 , 2)
    println(rdd15.partitions.size)
    val rdd16 = rdd15.repartition(3)
    println(rdd16.partitions.size)

    println("sortBy算子演示")
    val rdd17 = sc.parallelize(Array(3,2,5,1,5,0))
    rdd17.collect.foreach(print)
    val rdd18 = rdd17.sortBy(x => x)
    println
    rdd18.collect.foreach(print)
    val rdd19 = rdd18.sortBy(x => x,false)
    println
    rdd19.foreach(print)

    println
    println("union算子演示")
    val rdd20 = sc.parallelize(0 to 3)
    rdd20.collect.foreach(print)
    val rdd21 = sc.parallelize(0 to 5)
    println
    rdd21.collect.foreach(print)
    val rdd22 = rdd20.union(rdd21)
    println
    rdd22.collect.foreach(print)
    println
    println("subtract算子演示")
    val rdd23 = sc.parallelize(0 to 10)
    rdd23.collect.foreach(print)
    val rdd24 = sc.parallelize(2 to 6)
    println
    rdd4.collect.foreach(print)
    val rdd25 = rdd24.subtract(rdd23)
    println
    rdd25.collect.foreach(print)
    println
    println("intersection算子演示")

    val rdd26 = sc.parallelize(0 to 10)
    rdd26.collect.foreach(print)
    val rdd27 = sc.parallelize(5 to 20)
    println
    rdd27.collect.foreach(print)
    val rdd28 = rdd27.intersection(rdd26)
    println
    rdd28.collect.foreach(print)
    println
    println("reduceyKey算子演示")

    val rdd29 = sc.parallelize(Array((1,2),(1,4),(2,3),(2,4)))
    rdd29.collect.foreach(print)
    val rdd30 = rdd29.reduceByKey(_ + _)
    println
    rdd30.collect.foreach(print)
    println
    println("groupByKey算子演示")

    val rdd31 = sc.parallelize(Array((1,2),(1,4),(2,3),(2,4)))
    rdd31.collect.foreach(print)
    val rdd32 = rdd31.groupByKey()
    println
    rdd32.collect.foreach(print)
    println

    println("sortByKey算子演示")

    val rdd33 = sc.parallelize(Array((3,3),(3,1),(1,4),(1,2),(2,4),(2,3)))
    rdd33.collect.foreach(print)
    val rdd34 = rdd33.sortByKey()
    println
    rdd34.collect.foreach(print)
    val rdd35 = rdd34.sortByKey(false)
    println
    rdd35.collect.foreach(print)
    println
    println("mapValues算子演示")

    val rdd36 = sc.parallelize(Array((1,1),(1,2),(1,3),(2,1),(2,2),(2,3)))
    rdd36.collect.foreach(print)
    val rdd37 = rdd36.mapValues(_ * 2)
    println
    rdd37.collect.foreach(print)
    println

    println("join算子演示")

    val rdd38 = sc.parallelize(Array((1,1),(1,2),(2,1),(2,2)))
    rdd38.collect.foreach(print)
    val rdd39 = sc.parallelize(Array((2,3),(2,4),(3,5),(3,6)))
    println
    rdd39.collect.foreach(print)
    val rdd40 = rdd38.join(rdd39)
    println
    rdd40.collect.foreach(print)
    println
    println("collect算子演示")

    val rdd41 = sc.parallelize(List(1,2,3,4))
    rdd41.collect.foreach(print)
    println

    println("count算子演示")

    val rdd42 = sc.parallelize(0 to 9)
    rdd42.collect.foreach(print)
    println
    println(rdd42.count)


    println("first算子演示")

    val rdd43 = sc.parallelize(Array("spark","shell","first"))
    rdd43.collect.foreach(print)
    println(rdd43.first)

    println("take算子演示")

    val rdd44 = sc.parallelize(Array(3,1,4,2,6,5))
    rdd44.collect.foreach(print)
    println
    rdd44.take(3).foreach(print)
    println()
    println("takeOrdered算子演示")

    val rdd45 = sc.parallelize(Array(3,1,4,2,6,5))
    rdd45.collect.foreach(print)
    println
    rdd45.takeOrdered(3).foreach(print)
    println
    println("saveAsTextFile算子演示")

    val rdd46 = sc.parallelize(Array("spark","shell","saveToTextFile"))
    rdd46.saveAsTextFile("hdfs://master1:8020/output/2")
    println("可使用命令 hadoop fs -cat /output/2/* 查看是否保存到hdfs上")

    println("countByKey算子演示")

    val rdd47 = sc.parallelize(Array((1,2),(1,3),(1,5),(2,4),(2,6),(3,7)))
    rdd47.collect.foreach(print)
    val rdd48 = rdd47.countByKey
    println
    rdd48.foreach(print)
    println
    println("foreach算子演示")

    val rdd49 = sc.parallelize(1 to 10)
    rdd49.foreach(print)
    sc.stop()
  }
}
