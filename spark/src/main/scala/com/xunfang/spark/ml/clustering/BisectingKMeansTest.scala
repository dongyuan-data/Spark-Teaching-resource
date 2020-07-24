package com.xunfang.spark.ml.clustering

import org.apache.spark.ml.clustering.BisectingKMeans
import org.apache.spark.sql.SparkSession

object BisectingKMeansTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("BisectingKMeansTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()

    val df = spark.read.format("libsvm")
      .load("hdfs://master1:8020/sparkdata/sample_kmeans_data.txt")

    //实例化一个Bisecting k-means的估算器
    val bkm = new BisectingKMeans().setK(2).setSeed(1)

    val model = bkm.fit(df)

    //使用df得到该模型的误差平方和
    val cost = model.computeCost(df)
    println("查看误差平方和")
    println(cost)
    println("查看数据中心")
    model.clusterCenters.foreach(println)
  }
}
