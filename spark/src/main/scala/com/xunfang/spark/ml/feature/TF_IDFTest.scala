package com.xunfang.spark.ml.feature

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
object TF_IDFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("TF_IDFTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    println("查看原始数据集")
    df.show(false)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    val df1 = tokenizer.transform(df)
    println("打印转换后的数据集")
    df1.show(false)
    val f = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    val df2 = f.transform(df1)
    println("查看单词数组和特征向量的关系")
    df2.select("words","rawFeatures").show(false)
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val model = idf.fit(df2)
    val df3 = model.transform(df2)
    println("产看结果")
    df3.select("label","features").show(false)
  }
}
