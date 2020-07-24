package com.xunfang.spark.ml.classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.sql.SparkSession

object RandomForestClassificationTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("RandomForestClassificationTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    //读取数据
    val df = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    //自动识别分类特征和索引,同时设置最大分类,具有大于4个不同的值的特征视为连续,由参数setMaxCategories()决定
    val indexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(df)

    //拆分数据集分城训练数据集70%,和测试数据集30%
    val Array(trainingData,testData) = df.randomSplit(Array(0.7,0.3))

    //训练随机森分类模型,同时可以指定.setNumTrees(num)参数指定需要多少棵决策树num替换为数字
    val rfc = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("indexedFeatures")

    //定义管道
    val pipeline = new Pipeline().setStages(Array(indexer,rfc))

    //训练模型
    val model1 = pipeline.fit(trainingData)

    //预测
    val df1 = model1.transform(testData)

    //显示预测结果前10行
    df1.select("prediction","label","features").show(10)

    //对预测分类的列名和真实分类的列名进行设置
    val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")

    //计算测试数据的均方根误差(均方根误差就是标准差,是方差的算术平方根)简称RMSE
    val rmse = evaluator.evaluate(df1)

    println("均方根误差为:" + rmse)

    //通过mode1来获取随机森林模型
    val rfcm = model1.stages(1).asInstanceOf[RandomForestClassificationModel]

    println("随机森林模型为 : \n" + rfcm.toDebugString)
  }
}
