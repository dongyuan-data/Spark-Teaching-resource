package com.xunfang.spark.streaming


import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable


object DStreamFilter {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DStreamFilter").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //生成可变Int类型RDD队列
    val rdd1 = new mutable.Queue[RDD[Int]]()
    rdd1.foreach(println)
    //利用rdd1生成DStream
    val value = ssc.queueStream(rdd1, false)
    //筛选DStream中rdd元素大于五的元素
    val value1 = value.filter(_ > 5)
    value1.print()
    ssc.start()
    //创建一个从1到10的rdd赋值给rdd1
    rdd1 += ssc.sparkContext.makeRDD(1 to 10)
    //此线程休眠2000ms
    Thread.sleep(2000)
    ssc.awaitTermination()
  }
}
