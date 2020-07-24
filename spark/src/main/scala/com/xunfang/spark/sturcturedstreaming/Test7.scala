package com.xunfang.spark.sturcturedstreaming

import org.apache.spark.sql.SparkSession

object Test7 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test7").master("local[2]").getOrCreate()
    val frame = spark
      .readStream
      .format("socket")
      .option("host", "master1")
      .option("port", "10000")
      .option("includeTimestamp", "true")
      .load()
    frame.writeStream
      .format("csv")
      .outputMode("append")
      .option("checkpointLocation", "hdfs://master1:8020/checkpoint")
      .option("path","hdfs://master1:8020/aa")
      .start()
      .awaitTermination()
  }
}
