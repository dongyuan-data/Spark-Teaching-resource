package com.xunfang.spark.pipeline

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.SparkSession

object PipelineTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("PipelineTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    //原始数据集用于训练模型
    val training = spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    //创建分词器并设置参数
    val tokenizer = new Tokenizer()

    //创建HashingTF,计算词频
    val hashingTF = new HashingTF()

    //创建逻辑回归算法实例
    val lr = new LogisticRegression()

    //创建管道,有三个阶段
    val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))

    //创建参数列表
    val map = ParamMap(tokenizer.inputCol -> "text", tokenizer.outputCol -> "words",
      hashingTF.numFeatures -> 1000, hashingTF.inputCol -> "words",
      hashingTF.outputCol -> "features", lr.maxIter -> 10, lr.regParam -> 0.001)
    //使用原始数据集训练管道模型
    val model = pipeline.fit(training,map)

    //将管道模型保存到hdfs上(路径不存在需要自己创建)
    model.write.overwrite().save("hdfs://master1:8020/model/1")

    //将管道保存到hdfs上(路径不存在需要自己创建)
    pipeline.write.overwrite().save("hdfs://master1:8020/pipeline/1")

    //创建测试数据集,本数据没有标签.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "spark hadoop spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    //使用管道模型对测试数据集添加标签
    val frame = model.transform(test)

    //查看
    println("查看预测结果")
    frame.select("id","text","prediction").show()

    val htf = model.stages(1).asInstanceOf[HashingTF]

    //获取HashingTF的numFeatures参数
    println("获取HashingTF的numFeatures参数:")
    println(htf.getNumFeatures)
    val lrModel = model.stages(2).asInstanceOf[LogisticRegressionModel]

    //获取LogisticRegression的参数
    println("获取LogisticRegression的参数")
    println(lrModel.parent.explainParams())

    val model1 = PipelineModel.load("hdfs://master1:8020/model/1")
    val pipeline1 = Pipeline.load("hdfs://master1:8020/pipeline/1")
    val frame1 = model1.transform(test)
    println("读取储存的模型进行测试打印结果")
    frame1.select("id","text","prediction").show()
    println("显示管道的各个阶段")
    println("stage1为:" + pipeline1.getStages(0))
    println("stage2为:" + pipeline1.getStages(1))
    println("stage3为:" + pipeline1.getStages(2))
  }
}
