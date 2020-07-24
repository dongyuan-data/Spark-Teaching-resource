package com.xunfang.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Matrix, Vectors,Vector}
import org.apache.spark.ml.stat.{ChiSquareTest, Correlation}
import org.apache.spark.sql.Row
import org.apache.spark.ml.stat.Summarizer
import Summarizer._
object BasicStatistics {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("BasicStatistics")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    println("----------------------------------------------------------------")
    println("相关性程序演示")
    val data = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0))))
      import spark.implicits._
    val df = data.map(Tuple1.apply).toDF("features")
    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
    println("使用皮尔曼相关系数计算")
    println(coeff1.toString())
    println("使用皮尔曼等级相关系数计算")
    println(coeff2.toString())
    println("----------------------------------------------------------------")
    println("独立性校验程序演示")

    val data1 = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )
    val df1 = data1.toDF("label", "features")
    val chi = ChiSquareTest.test(df1, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")
    println("----------------------------------------------------------------")
    println("总结器程序演示")
    val data2 = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )

    val df2 = data2.toDF("features", "weight")

    val (meanVal, varianceVal) = df2.select(metrics("mean", "variance").summary($"features", $"weight").as("summary")).select("summary.mean","summary.variance").as[(Vector, Vector)].first()

    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")

    val (meanVal2, varianceVal2) = df2.select(mean($"features"), variance($"features")).as[(Vector, Vector)].first()

    println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")
  }
}
