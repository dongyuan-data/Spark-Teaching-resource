package com.xunfang.spark.streaming

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ForeachRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ForeachRDD").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val scc = new StreamingContext(sc, Seconds(5))
    scc.checkpoint("hdfs://master1:8020/checkpoint")

    val textFile = scc.socketTextStream("master1", 10000)

    val mapValue = textFile.flatMap(_.split(" ")).map((_, 1))

    val reduce = mapValue.reduceByKeyAndWindow((a: Int, b: Int) => (a + b), (a: Int, b: Int) => (a - b), Seconds(5), Seconds(5))

    reduce.foreachRDD( rdd => {
      rdd.foreach( a => {
        Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
        val connection = DriverManager.getConnection("jdbc:mysql://master1:3306/spark", "root", "123456")
        val prep = connection.prepareStatement("insert into sparkstreaming (word,count) values(? ,?)")
        prep.setString(1,a._1.toString)
        prep.setLong(2,a._2)
        prep.executeUpdate
        connection.close()
      })
    })
    scc.start()
    scc.awaitTermination()
  }
}
