package com.xunfang.spark.ml.tuning

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object CrossValidatorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("CrossValidatorTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0),
      (4L, "b spark who", 1.0),
      (5L, "g d a y", 0.0),
      (6L, "spark fly", 1.0),
      (7L, "was mapreduce", 0.0),
      (8L, "e spark program", 1.0),
      (9L, "a e c l", 0.0),
      (10L, "spark compile", 1.0),
      (11L, "hadoop software", 0.0)
    )).toDF("id", "text", "label")

    //实例化一个Tokenizer，将text列文本进行分隔，间隔符为“ ”
    val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")

    //实例化一个HashingTF，将Tokenizer估算器的输出列转化成特征向量
    val hashingTF = new HashingTF().setInputCol(tokenizer.getOutputCol).setOutputCol("features")

    //实例化一个逻辑回归算法估算器
    val lr = new LogisticRegression().setMaxIter(10)

    //实例化一个管道估算器，管道有三个阶段。
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //利用ParamGridBuilder创建一个参数表格，参数包括HashingTF的numFeatures和LogisticRegression的reParam
    val paramGrid = new ParamGridBuilder().addGrid(hashingTF.numFeatures, Array(10, 100, 1000)).addGrid(lr.regParam, Array(0.1, 0.01)).build()

    //实例化一个交叉验证，设置估算器为管道估算器这样参数表格中的参数就会运用于整个管道，设置评估器为二元分类评估器（因为本实验属于二元分类问题），设置参数表格为paramGrid，设置折数为2，设置并行评估参数最多为2个
    val cv = new CrossValidator().setEstimator(pipeline).setEvaluator(new BinaryClassificationEvaluator).setEstimatorParamMaps(paramGrid).setNumFolds(2).setParallelism(2)

    //训练模型
    val cvModel= cv.fit(df)

    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    //测试模型
    val frame = cvModel.transform(test)

    //打印测试数据集转换后的数据集
    println("打印测试数据集转换后的数据集")
    frame.select("id", "text", "probability", "prediction").show(false)

    //通过交叉验证模型获取管道模型
    val ppModel = cvModel.bestModel.asInstanceOf[PipelineModel]

    //通过管道模型获取实例化的HashingTF
    val hTF = ppModel.stages(1).asInstanceOf[HashingTF]

    //打印HashingTF选取的最优参数
    println("打印HashingTF选取的最优参数")
    println(hTF.getNumFeatures)

    //通过管道模型获取逻辑回归模型
    val lrmodel = ppModel.stages(2).asInstanceOf[LogisticRegressionModel]

    //打印逻辑回归模型选取的最优参数
    println("打印逻辑回归模型选取的最优参数")
    println(lrmodel.getRegParam)

    //打印逻辑回归模型的所有参数
    println("打印逻辑回归模型的所有参数")
    println(lrmodel.explainParams)
  }
}
