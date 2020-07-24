package com.xunfang.spark.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TransformTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TransformTest").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    val fileText = ssc.socketTextStream("master1", 10000)
    val mapValue = fileText.flatMap(_.split(" ")).map((_, 1))
    //使用transform算子改变广播变量
    val unit = mapValue.transform(rdd => {
      //定义广播变量
      val asList = Array("a", "b")
      //将广播变量进行广播
      val blackList = ssc.sparkContext.broadcast(asList)
      //获取广播变量,将不再广播变量中的数据过滤掉
      val transformRDD = rdd.filter(line => {
        blackList.value.contains(line._1)
      })
      //将过滤后的数据进行聚合统计
      val reduce = transformRDD.reduceByKey(_ + _)
      reduce
    })
    unit.print
    ssc.start()
    ssc.awaitTermination()

  }
}
