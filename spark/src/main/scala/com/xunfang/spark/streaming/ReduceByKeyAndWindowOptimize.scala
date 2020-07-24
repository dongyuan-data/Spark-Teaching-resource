package com.xunfang.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ReduceByKeyAndWindowOptimize {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByKeyAndWindowOptimize").setMaster("local[2]")
    val scc = new StreamingContext(conf, Seconds(5))
    scc.checkpoint("hdfs://master1:8020/checkpoint")
    val textFile = scc.socketTextStream("master1", 10000)
    val mapValue = textFile.flatMap(_.split(" ")).map((_, 1))
    //第一个参数是聚合函数,第二个参数是当数据重复出现时不进行重复计算
    val reduce = mapValue.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), (a: Int, b: Int) => (a - b), Seconds(20), Seconds(5))
    reduce.print
    scc.start()
    scc.awaitTermination()
  }
}
