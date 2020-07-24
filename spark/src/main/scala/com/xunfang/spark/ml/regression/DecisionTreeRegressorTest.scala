package com.xunfang.spark.ml.regression

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor

object DecisionTreeRegressorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("DecisionTreeRegressorTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val data = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    //自动识别分类特征和索引，设置输入列为features，输出列为indexedFeatures，同时设置最大分类，具有大于4个不同的值的特征视为连续，利用data数据集训练模型
    val fi = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    // 将原始数据集切分成两部分一部分用来训练模型一部分用来测试模型
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 实例化一个决策树回归估算器，设置标签列为label设置特征列为indexedFeatures
    val dt = new DecisionTreeRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures")

    // 实例化一个管道，有两个阶段fi和dt
    val pipeline = new Pipeline().setStages(Array(fi, dt))

    // 利用训练数据集训练管道路径
    val model = pipeline.fit(trainingData)

    // 使用测试数据测试模型
    val df = model.transform(testData)

    // 查询结果
    df.select("prediction", "label", "features").show(5)

    // 实例化一个回归统计器设置标签列为label设置预测列为prediction设置结果列为rmse
    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

    //得到结果
    val rmse = evaluator.evaluate(df)

    //结果越偏向0说明误差越小
      println("均方根误差为：" + rmse)

    //得到决策树模型
    val treeModel = model.stages(1).asInstanceOf[DecisionTreeRegressionModel]

    //打印决策树模型
    println(treeModel.toDebugString)
  }
}
