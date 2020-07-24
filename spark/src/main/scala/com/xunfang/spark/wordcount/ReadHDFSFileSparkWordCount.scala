package com.xunfang.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object ReadHDFSFileSparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ReadHDFSFileSparkWordCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = sc.textFile("hdfs://master1:8020/user/root/data/sparkWordCount")
    val rdd1 = arr.flatMap(_.split(" "))
    val rdd2 = rdd1.flatMap(_.split(","))
    val rdd3 = rdd2.map((_,1))
    val rdd4 = rdd3.reduceByKey(_ + _)
    val rdd5 = rdd4.sortBy(_._2,false)
    rdd5.take(3).foreach(print)
    sc.stop()
  }
}
