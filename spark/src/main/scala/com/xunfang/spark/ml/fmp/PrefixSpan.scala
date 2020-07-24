package com.xunfang.spark.ml.fmp

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.fpm.PrefixSpan

object PrefixSpan {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("PrefixSpan")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()

    val df = spark.createDataFrame(Seq(
      Seq(Seq(1, 2), Seq(3)),
      Seq(Seq(1), Seq(3, 2), Seq(1, 2)),
      Seq(Seq(1, 2), Seq(5)),
      Seq(Seq(6))).map(Tuple1.apply)).toDF("sequence")

    val span = new PrefixSpan().setMinSupport(0.5).setMaxPatternLength(5).setMaxLocalProjDBSize(32000000)
    println("展示结果")
    span.findFrequentSequentialPatterns(df).show()
  }
}
