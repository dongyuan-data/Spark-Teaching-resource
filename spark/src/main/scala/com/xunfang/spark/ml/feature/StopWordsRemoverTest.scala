package com.xunfang.spark.ml.feature

import org.apache.spark.ml.feature.{StopWordsRemover, Tokenizer}
import org.apache.spark.sql.SparkSession
object StopWordsRemoverTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("StopWordsRemoverTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val df = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")
    println("产看原始数据")
    df.show(false)
    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("word")
    val df1 = tokenizer.transform(df)
    val remover = new StopWordsRemover().setStopWords(Array("I","Java"))
      .setInputCol("word").setOutputCol("text")
    val frame = remover.transform(df1)
    println("查看转换后的数据")
    frame.show(false)
  }
}
