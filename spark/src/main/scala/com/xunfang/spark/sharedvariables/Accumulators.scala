package com.xunfang.spark.sharedvariables

import org.apache.spark.{SparkConf, SparkContext}

object Accumulators {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulators").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///root/data/t1.txt")
    val acc = sc.longAccumulator("My Accumulator")
    val rdd1 = rdd.flatMap(_.split(""))
    val rdd2 = rdd1.flatMap(word =>{if(word == "s"){acc.add(1)};word})
    println("t1.txt文本内容：")
    rdd2.collect.foreach(print)
    println
    println("累加器的值为：" + acc.value)
    sc.stop()
  }
}
