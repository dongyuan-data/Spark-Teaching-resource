package com.xunfang.spark.ml

import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.sql.SparkSession

object ToDebugStringTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("ToDebugStringTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val frame = spark.read.format("libsvm")
      .load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")
    val dtc = new DecisionTreeClassifier().setLabelCol("label").setFeaturesCol("features")
    val model = dtc.fit(frame)
    println("打印决策树模型")
    println(model.toDebugString)
  }
}
