package com.xunfang.spark.ml.clustering

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

object KMeansTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("KMeansTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()

    val df = spark.read.format("libsvm")
      .load("hdfs://master1:8020/sparkdata/sample_kmeans_data.txt")

    // 实例化一个K-means的估算器设置k为2，设置可重复随机（setSeed的括号内可以是任意值对结果无影响）
    val kmeans = new KMeans().setK(2).setSeed(1L)

    // 利用原始数据训练k-means模型
    val model = kmeans.fit(df)

    // 将原始数据进行聚类，转化成另一个数据集
    val df1 = model.transform(df)

    // 生成一个聚类估算器
    val evaluator = new ClusteringEvaluator()

    //对df1进行分析生成结果，得到平均欧氏距离（用于衡量各个簇在空间上的距离，距离越远说明差异越大）
    val silhouette = evaluator.evaluate(df1)
    println("得到平均欧式距离")
    println(silhouette)
    println("打印数据集进行聚类后的中心")
    //打印数据集进行聚类后的中心
    model.clusterCenters.foreach(println)
  }
}
