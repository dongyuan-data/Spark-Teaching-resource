package com.xunfang.spark.sparkbasis

import org.apache.spark.{SparkConf, SparkContext}


object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sparkWorkCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.textFile("hdfs://master1:8020/user/root/data/text1.txt")
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .collect()
      .foreach(println)
    sc.stop()
  }
}
