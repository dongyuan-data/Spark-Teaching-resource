package com.xunfang.spark.ml.feature
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
object Word2VecTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("Word2VecTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()

    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    println("查看原始数据集")
    documentDF.show(false)

    val word2Vec = new Word2Vec()
      .setInputCol("text").setOutputCol("result")
      .setVectorSize(3).setMinCount(0)
    val model = word2Vec.fit(documentDF)
    val result = model.transform(documentDF)
    println("统计词频")
    result.show(false)
  }
}
