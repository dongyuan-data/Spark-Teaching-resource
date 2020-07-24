package com.xunfang.spark.ml.feature

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors

object ChiSqSelectorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("ChiSqSelectorTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      (8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      (9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0))
    ).toDF("id", "features", "clicked")
    println("查看原始数据集")
    df.show(false)
    val selector = new ChiSqSelector().setNumTopFeatures(1).setFeaturesCol("features")
      .setLabelCol("clicked").setOutputCol("selectedFeatures")
    val model = selector.fit(df)
    val frame = model.transform(df)
    println(s"卡方特征选择器选择前 ${selector.getNumTopFeatures} 个特征")
    println("查看结果")
    frame.show(false)

  }
}
