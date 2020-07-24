package com.xunfang.spark.sharedvariables

import org.apache.spark.{SparkConf, SparkContext}

object BroadcastVariables {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("BroadcastVariables")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)
    val broadcast = sc.broadcast(Array(1,2,3,4,5))
    print("广播变量值为：")
    broadcast.value.foreach(print)
    sc.stop()
  }
}
