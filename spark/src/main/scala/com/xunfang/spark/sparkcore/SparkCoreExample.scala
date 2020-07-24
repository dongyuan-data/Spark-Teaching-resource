package com.xunfang.spark.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SparkCoreExample")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    println("第一个问题演示")
    val rowRDDA = sc.textFile("file:///root/data/rowRDDA")
    //val rowRDDA = sc.textFile("hdfs://master1:8020/data/rowRDDA")
    val rdd1 = rowRDDA.flatMap(_.split(" "))
    val rdd2 = rdd1.map((_,1))
    val rdd3 = rdd2.reduceByKey(_ + _)
    val aa = rdd3.filter(_._1 == "aa")
    println("aa有多少个")
    aa.collect.foreach(print)
    val bb = rdd3.filter(_._1 == "bb")
    println
    println("bb有多少个")
    bb.collect.foreach(print)
    println
    println("第二个问题演示")
    val rowRDDB = sc.textFile("file:///root/data/rowRDDB")
    //val rowRDDB = sc.textFile("hdfs://master1:8020/data/rowRDDB")
    val rdd4 = rowRDDB.flatMap(_.split(" "))
    val rdd5 = rowRDDA.flatMap(_.split(" ")).distinct
    val rdd6 = rdd5.subtract(rdd4)
    rdd6.collect.foreach(println)
    println("第三个问题演示")
    val aavalue = aa.map(word => word._1 + "," + word._2)
    val bbvalue = bb.map(word => word._1 + "," + word._2)
    val union = aavalue.union(bbvalue).union(rdd6)
    println("合并后的结果")
    union.collect.foreach(println)
    union.saveAsTextFile("hdfs://master1:8020/output/2")
    val save = sc.textFile("hdfs://master1:8020/output/2")
    println("读取hdfs中的结果")
    save.collect.foreach(println)
    sc.stop()
  }
}
