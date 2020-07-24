package com.xunfang.spark.pipeline

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.param.ParamMap

object ParamTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
      (1.0, Vectors.dense(0.0, 1.2, -0.5))
    )).toDF("label", "features")
    val lr = new LogisticRegression()
    lr.setMaxIter(10).setRegParam(0.01).setLabelCol("label").setFeaturesCol("features")
    val model = lr.fit(df)
    println(model.parent.extractParamMap)
    val lr1 = new LogisticRegression()
    val lr2 = new LogisticRegression()
    lr1.setMaxIter(15).setRegParam(0.01)
    lr2.setMaxIter(5).setRegParam(0.02)
    //第三个参数是修改列名的
    val map = ParamMap(lr1.maxIter -> 10,lr1.regParam -> 0 ,lr1.predictionCol -> "aaa",lr2.maxIter -> 20,lr2.regParam -> 0.05 )
    val model1 = lr1.fit(df,map)
    val model2 = lr2.fit(df,map)
    println("lr1的参数\n" + model1.parent.explainParams())
    println("\n--------------------")
    println("lr2的参数\n" + model2.parent.explainParams())
  }
}
