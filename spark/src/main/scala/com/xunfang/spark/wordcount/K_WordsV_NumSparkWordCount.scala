package com.xunfang.spark.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object K_WordsV_NumSparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("K_WordsV_NumSparkWordCount")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val arr = sc.parallelize(Array(("hello word", 2), ("spark word count", 4), ("xunfang", 6), ("wordCount spark", 8)))
    val rdd = arr.flatMap{ t => { val strings = t._1.split(" ");strings.map(a =>(a,t._2)) }}
    val rdd1 = rdd.reduceByKey(_ + _)
    val rdd2 = rdd1.sortBy(_._2,false)
    rdd2.collect.foreach(print)
    sc.stop()
  }
}
