package com.xunfang.spark.ml.feature

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
import org.apache.spark.sql.SparkSession
object IndexToStringTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("IndexToStringTest")
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
    df.show(false)
    val indexer = new StringIndexer().setInputCol("text").setOutputCol("indexer").fit(df)
    val frame = indexer.transform(df)
    val string = new IndexToString().setInputCol("indexer").setOutputCol("String")
    val df1 = string.transform(frame)
    println("打印转换后数据集")
    df1.show(false)
  }
}
