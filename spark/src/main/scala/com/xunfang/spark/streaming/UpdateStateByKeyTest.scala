package com.xunfang.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object UpdateStateByKeyTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("updateStateByKeyTest").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))
    //设置检查点
    ssc.checkpoint("hdfs://master1:8020/checkpoint")
    val fileText = ssc.socketTextStream("master1", 10000)
    val mapValue = fileText.flatMap(_.split(" ")).map((_, 1))
    /*利用updateStateByKey进行跨批次统计
    第一个参数是本次要处理的value值,第二个参数对应的是上一个阶段已经求和后的结果
    */
    val update = mapValue.updateStateByKey((value: Seq[Int], state: Option[Int]) => {
      /*利用当前阶段相同key对应value值的和,和上一阶段该key对应的value值的和求和的到新的和
      */
      val newCount = value.sum + state.getOrElse(0)
      Some(newCount)
    })
    update.print
    ssc.start()
    ssc.awaitTermination()
  }
}
