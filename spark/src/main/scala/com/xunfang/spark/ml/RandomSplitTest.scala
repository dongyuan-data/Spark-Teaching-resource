package com.xunfang.spark.ml

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.Vectors

object RandomSplitTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("RandomSplitTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()

    val df = spark.createDataFrame(Seq(
      Vectors.dense(0.0, 3.0, 1.0),
      Vectors.dense(1.0, 2.0, 2.0),
      Vectors.dense(2.0, 3.0, 3.0),
      Vectors.dense(1.0, 2.0, 1.0),
      Vectors.dense(2.0, 1.0, 3.0)
    ).map(Tuple1.apply)).toDF("features")
    println("将数据集分成两部分")
    val Array(train,test) = df.randomSplit(Array(0.6,0.4))
    train.show
    test.show
    println("将数据集分成三部分")
    val Array(aa,bb,cc) = df.randomSplit(Array(0.6,0.2,0.2))
    aa.show
    bb.show
    cc.show
  }
}
