package com.xunfang.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object KV_WordSparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("KV_WordSparkWordCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.parallelize(Array(("spark", "wordcount"), ("spark", "wordcount"), ("spark", "spark")))
    val rdd1 = rdd.map( t => (t._1,1))
    val rdd2 = rdd.map( t => (t._2,1))
    val rdd3 = rdd1.union(rdd2)
    val rdd4 = rdd3.reduceByKey(_ + _)
    val value = rdd4.sortBy(_._2,false)
    value.collect.foreach(print)
    sc.stop()
  }
}
