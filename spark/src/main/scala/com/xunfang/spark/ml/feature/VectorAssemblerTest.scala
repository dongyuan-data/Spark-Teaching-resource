package com.xunfang.spark.ml.feature

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession
object VectorAssemblerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("VectorAssemblerTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (1, 1, 5),
      (2, 2, 0),
      (2, 3, 5),
      (1, 4, 0),
      (3, 5, 0)
    )).toDF("a", "b", "c")
    println("打印原始数据集")
    df.show(false)
    val assembler = new VectorAssembler()
      .setInputCols(Array("a","b","c")).setOutputCol("features")
    val frame = assembler.transform(df)
    println("打印转换后数据集")
    frame.show(false)
  }
}
