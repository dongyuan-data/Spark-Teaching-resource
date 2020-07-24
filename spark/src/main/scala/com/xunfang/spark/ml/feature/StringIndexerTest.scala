package com.xunfang.spark.ml.feature

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession


object StringIndexerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "d"),
      (4, "e"),
      (5, "a"),
      (6, "e")
    )).toDF("id" , "text")
    println("查看原始数据集")
    val indexer = new StringIndexer().setInputCol("text").setOutputCol("indexer").fit(df)
    val frame = indexer.transform(df)
    println("打印转换后数据集")
    frame.show(false)
  }
}
