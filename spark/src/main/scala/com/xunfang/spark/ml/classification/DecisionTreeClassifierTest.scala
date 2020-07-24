package com.xunfang.spark.ml.classification

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SparkSession

object DecisionTreeClassifierTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("DecisionTreeClassifierTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    //读取全部数据
    val df = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")

    //把元数据添加到,整个数据集中所有的索引标签的标签列中
    val mode1 = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)

    //自动识别分类特征和索引,同时设置最大分类,具有大于4个不同的值的特征视为连续,由参数setMaxCategories()决定
    val mode2 = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(df)

    //拆分数据集分城训练数据集70%,和测试数据集30%
    val Array(traniningData,testData) = df.randomSplit(Array(0.7,0.3))

    //训练决策树分类模型
    val dtc = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")

    //将索引标签强制转化成原始标签
    val label = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(mode1.labels)

    //定义管道将个步骤进行连接
    val pip = new Pipeline().setStages(Array(mode1,mode2,dtc,label))

    //训练模型
    val mode3 = pip.fit(traniningData)

    //预测
    val df1 = mode3.transform(testData)

    //显示选定列的前20行内容
    println("查看训练后的数据")
    df1.select("predictedLabel","label","features").show(20)

    //对预测分类的列名和真实分类的列名进行设置
    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")

    //评估准确率
    val accuracy = evaluator.evaluate(df1)
    println("准确率:" + accuracy)

    //通过mode3来获取决策树模型
    val mode4 = mode3.stages(2).asInstanceOf[DecisionTreeClassificationModel]

    println("决策树模型:\n" + mode4.toDebugString)
  }
}
