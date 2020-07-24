package com.xunfang.spark.ml.tuning

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}

object TrainValidationSplitTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("TrainValidationSplitTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.read.format("libsvm")
      .load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    val Array(training, test) = df.randomSplit(Array(0.9, 0.1), seed = 12345)

    val lr = new LogisticRegression().setMaxIter(10)

    //setElasticNetParam:正则化范式比(默认0.0)取值范围[0,1]，正则化一般有两种范式：L1(Lasso)和L2(Ridge)。L1一般用于特征的稀疏化，L2一般用于防止过拟合。这里的参数即设置L1范式的占比，默认0.0即只使用L2范式
    val paramGrid = new ParamGridBuilder().addGrid(lr.regParam, Array(0.1, 0.01)).addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0)).build()

    //实例化一个TrainValidationSplit使用逻辑回归作为估算器，使用逻辑评估器评估，使用参数表格paramGrid，将原始数据集80%分成训练数据，将原始数据集20%分成测试数据。
    val TVS = new TrainValidationSplit().setEstimator(lr).setEvaluator(new RegressionEvaluator).setEstimatorParamMaps(paramGrid).setTrainRatio(0.8).setParallelism(2)

    val model = TVS.fit(training)

    val frame = model.transform(test)
    println("获取转化后的测试数据")
    frame.select("features", "label", "prediction").show()

    val lrModel = model.bestModel.asInstanceOf[LogisticRegressionModel]

    //获取选择的最佳参数
    println("获取选择的最佳参数")
    println(lrModel.getRegParam)
    println(lrModel.getElasticNetParam)
  }
}
