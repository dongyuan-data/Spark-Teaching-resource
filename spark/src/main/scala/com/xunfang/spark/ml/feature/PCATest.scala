package com.xunfang.spark.ml.feature

import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

object PCATest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Array(
      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
    ).map(Tuple1.apply)).toDF("features")
    println("查看原始数据")
    df.show(false)
    val model = new PCA().setInputCol("features")
      .setOutputCol("pcaFeatures").setK(3).fit(df)
    val frame = model.transform(df)
    println("打印结果")
    frame.show(false)
  }
}
