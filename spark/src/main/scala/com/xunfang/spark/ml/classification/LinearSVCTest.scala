package com.xunfang.spark.ml.classification

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

object LinearSVCTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("LinearSVCTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    //切分数据集
    val Array(train,test) = df.randomSplit(Array(0.7,0.3))

    //实例化一个线性支持向量机的估算器，设置最大迭代数为10，正则化系数为0.1
    val sVC = new LinearSVC().setMaxIter(10).setRegParam(0.1)

    //训练模型
    val model = sVC.fit(train)

    //利用模型预测标签
    val frame = model.transform(test)

    //打印预测后数据集中10行的预测列，标签列，特征列数据
    frame.select("prediction","label","features").show(10)

    //实例化二分类统计器，设置标签列为label，设置预测列为prediction，设置指标名称为accuracy
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")

    //计算指标正确率
    val acc = evaluator.evaluate(frame)

    //打印结果
    println("准确率为" + acc)

  }
}
