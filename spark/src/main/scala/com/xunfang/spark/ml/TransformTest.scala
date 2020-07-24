package com.xunfang.spark.ml

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.sql.SparkSession

object TransformTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TransformTest")
      .master("local[2]")
      .enableHiveSupport().getOrCreate()

    val df = spark.createDataFrame(Array(
      (1.0, "hi hello java spark"),
      (2.0, "hello spark scala wings"),
      (3.0, "Flink python DataFrame DataSet")
    )).toDF("id","text")
    println("原数据集")
    df.show(false)
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("features")
    val frame = tokenizer.transform(df)
    println("转换后数据集")
    frame.show(false)

  }
}
