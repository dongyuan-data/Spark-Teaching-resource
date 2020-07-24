package com.xunfang.spark.sturcturedstreaming

import org.apache.spark.sql.SparkSession

object Test3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test3").master("local[2]").getOrCreate()
    val lines = spark.readStream
      .format("socket")
      .option("host", "master1")
      .option("port", "10000")
      .load()
    import spark.implicits._
    val flatmap = lines.flatMap(_.toString.split(" "))
    val map = flatmap.map((_, 1))
    val group = map.groupByKey(_._1)
    val count = group.count()
    count.createOrReplaceTempView("people")
    val frame = spark.sql("select * from people")
    frame.writeStream.outputMode("complete").format("console").start().awaitTermination()
  }
}
