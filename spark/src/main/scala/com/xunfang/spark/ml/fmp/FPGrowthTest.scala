package com.xunfang.spark.ml.fmp

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.fpm.FPGrowth

object FPGrowthTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("FPGrowthTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    import spark.implicits._
    val df = spark.createDataset(Seq(
      "1 2 5",
      "1 2 3 5",
      "1 2")
    ).map(_.split(" ")).toDF("items")

    val fpgrowth = new FPGrowth().setItemsCol("items").setMinSupport(0.5).setMinConfidence(0.6)

    val model = fpgrowth.fit(df)

    // 打印频繁项目集
    println("打印频繁项目集")
    model.freqItemsets.show()

    // 打印获取符合置信度的关联规则
    println("打印获取符合置信度的关联规则")
    model.associationRules.show()

    // 使用已有的关联规则检查输入项汇总，进行预测然后打印出来
    println("使用已有的关联规则检查输入项汇总，进行预测然后打印出来")
    model.transform(df).show()
  }
}
