package com.xunfang.spark.ml.regression

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

object GBTRegressorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val data = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    val fi = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)

    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // 实例化一个梯度提升回归树估算器，设置标签列为label设置特征列为indexedFeatures
    val gbt = new GBTRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxIter(10)

    val pipeline = new Pipeline().setStages(Array(fi, gbt))

    val model = pipeline.fit(trainingData)

    val df = model.transform(testData)

    df.select("prediction", "label", "features").show(5)

    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

    val rmse = evaluator.evaluate(df)

    println("均方根误差为：" + rmse)

    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]

    println(gbtModel.toDebugString)
  }
}
