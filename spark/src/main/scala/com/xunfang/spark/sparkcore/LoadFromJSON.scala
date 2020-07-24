package com.xunfang.spark.sparkcore

import org.apache.spark.sql.SparkSession

object LoadFromJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadFromJSON").master("local[2]").getOrCreate()
    println("读取json1文件并显示")
    spark.read.format("json").load("hdfs://master1:8020/data/json1").show

    println("读取json2文件并显示")
    spark.read.format("json").load("hdfs://master1:8020/data/json2").show

    println("读取json3文件并显示")
    spark.read.format("json").load("hdfs://master1:8020/data/json3").show
    spark.stop()
  }
}
