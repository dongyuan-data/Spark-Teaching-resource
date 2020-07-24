package com.xunfang.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf


object ReduceByKeyAndWindow {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ReduceByKeyAndWindow").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("hdfs://master1:8020/checkpoint")
    val fileText = ssc.socketTextStream("master1",10000)
    val mapValue = fileText.map((_, 1))
    val count = mapValue.reduceByKeyAndWindow( (a:Int,b:Int) =>(a +b),Seconds(15),Seconds(10))
    count.print
    ssc.start()
    ssc.awaitTermination()
  }
}