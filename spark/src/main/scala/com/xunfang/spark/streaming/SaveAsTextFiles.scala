package com.xunfang.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SaveAsTextFiles {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByKeyAndWindowOptimize").setMaster("local[2]")
    val scc = new StreamingContext(conf, Seconds(5))
    scc.checkpoint("hdfs://master1:8020/checkpoint")
    val textFile = scc.socketTextStream("master1", 10000)
    val mapValue = textFile.flatMap(_.split(" ")).map((_, 1))
    val reduce = mapValue.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), (a: Int, b: Int) => (a - b), Seconds(5), Seconds(5))
    reduce.saveAsTextFiles("hdfs://master1:8020/output/","TXT")
    scc.start()
    scc.awaitTermination()
  }
}
