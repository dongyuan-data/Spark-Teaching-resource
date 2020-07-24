package com.xunfang.spark.sturcturedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp

object Test1 {
  def main(args: Array[String]): Unit = {
    //创建一个sparkSession
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Test1")
      .getOrCreate()
    import spark.implicits._
    //读取10000端口的数据
    val lines = spark.readStream
      //指定读取数据的方式是socket(计算机之间的进行通信的一种方式我们使用的netcat就是这种方式)
      .format("socket")
      .option("host", "master1")
      .option("port", "10000")
      //输入的内容包含了时间戳(Spakr收到数据的时间戳)
      .option("includeTimestamp", true)
      .load()
    //将输入的数据转换为[String,Timestamp]格式的DS,并将每行数据分割成单词，将单词当作事件的 sessionId返回成Event类型的DS
    val events = lines
      .as[(String, Timestamp)]
      .flatMap { case (line, timestamp) => // 模式匹配
        line.split(" ").map(word => Event(sessionId = word, timestamp))
      }
    //将events输出到控制台
    val query = events
      .writeStream
      //输出模式选择,只输出更新的数据,旧数据不显示
      .outputMode("update")
      //将数据输出到控制台
      .format("console")
      .start()
    query.awaitTermination()
  }
}
/** 用户自定义数据类型，表示输入事件 */
case class Event(sessionId: String, timestamp: Timestamp)
