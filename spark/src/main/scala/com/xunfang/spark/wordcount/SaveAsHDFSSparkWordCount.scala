package com.xunfang.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object SaveAsHDFSSparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SaveAsHDFSSparkWordCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = sc.textFile("hdfs://master1:8020/user/root/data/sparkWordCount")
    val rdd1 = arr.flatMap(_.split("[^a-zA-Z]"))
    val rdd2 = rdd1.map((_,1))
    val rdd3 = rdd2.reduceByKey(_ + _)
    val rdd4 = rdd3.sortBy(_._2)
    rdd4.saveAsTextFile("hdfs://master1:8020/root/output/3")
  }
}
