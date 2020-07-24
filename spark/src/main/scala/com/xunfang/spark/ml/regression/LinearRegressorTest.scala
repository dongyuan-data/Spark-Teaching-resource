package com.xunfang.spark.ml.regression

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.regression.LinearRegression

object LinearRegressorTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("LinearRegressorTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.read.format("libsvm").load("hdfs://master1:8020/sparkdata/sample_isotonic_regression_libsvm_data.txt")

    //实例化线性回归估算器，设置最大迭代次数100，设置正则化系数为0.1
    val regression = new LinearRegression().setMaxIter(100).setRegParam(0.1)

    //训练模型
    val model = regression.fit(df)
    println("回归系数为：" + model.coefficients + "\n回归截距为：" + model.intercept)

    //摘要统计
    val summary = model.summary

    //打印异常点
    summary.residuals.show(10,false)

    //结果越小说明误差越小
    println("均方根误差：" + summary.rootMeanSquaredError)

    //结果越接近1，说明所有数据越接近这条回归线
    println("判定系数：" + summary.r2)
  }
}
