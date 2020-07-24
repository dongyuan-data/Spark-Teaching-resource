package com.xunfang.spark.ml.clustering

import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.sql.SparkSession

object GaussianMixtureTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("GaussianMixtureTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.read.format("libsvm")
      .load("hdfs://master1:8020/sparkdata/sample_kmeans_data.txt")

    //实例化一个GMM估算器
    val gmm = new GaussianMixture().setK(2)

    val model = gmm.fit(df)

    // 输出GMM的决定因素
    println("输出GMM的决定因素")
    for (i <- 0 until model.getK) {println(s"Gaussian $i:\nweight=${model.weights(i)}\n" +
      s"mu=${model.gaussians(i).mean}\nsigma=\n${model.gaussians(i).cov}\n")
    }
  }
}
