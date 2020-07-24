package com.xunfang.spark.streaming

import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
  object StreamingWordCount {
    def main(args: Array[String]): Unit = {
        //设置参数
        val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)
        //创建SparkStreaming的入口,Seconds()表示每次处理数据的时间间隔
        val ssc = new StreamingContext(sc, Durations.seconds(5))
        //创建一个用于接收数据的DStream
        val textFile = ssc.socketTextStream("master1", 9999)
        //将DStream中的数据切分成一个个单词
        val word = textFile.flatMap(_.split("""\s+"""))
        //将单词转换成(k,v)结构数据
        val words = word.map((_, 1))
        //统计单词个数
        val count = words.reduceByKey(_ + _)
        //打印出来
        count.print
        //开始接收数据并计算
        ssc.start()
        //等待计算结束,只有出现了意外状况或者手动退出,才会退出主程序
        ssc.awaitTermination()
    }
  }
