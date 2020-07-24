package com.xunfang.spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable

object DStreamCountByValue {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DStreamTransformation").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val rdd1 = new mutable.Queue[RDD[Int]]()
    rdd1.foreach(println)
    val value = ssc.queueStream(rdd1, false)
    val value1 = value.map(("b", _))
    val value2 = value1.countByValue()
    value2.print()
    ssc.start()
    //循环创建rdd放入rdd队列中
    for (i <- 1 to 2) {
      rdd1 += ssc.sparkContext.makeRDD(1 to 10)
      Thread.sleep(2000)
    }
    ssc.awaitTermination()
    ssc.stop()
  }
}
