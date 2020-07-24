package com.xunfang.spark.ml.recommendation

import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS

object ALS {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("ALS")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    import spark.implicits._
    val file = spark.read
      .textFile("hdfs://master1:8020/sparkdata/als/sample_movielens_ratings.txt")
      .map(text=>{
      val strings = text.split("::")
      assert(strings.size == 4)
      (strings(0).toInt,strings(1).toInt,strings(2).toFloat,strings(3).toLong)
    }).toDF("userId","movieId","rating","timestamp")

    val Array(train, test) = file.randomSplit(Array(0.8, 0.2))

    val als = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    val model = als.fit(train)

    model.setColdStartStrategy("drop")

    val frame = model.transform(test)

    //实例化一个回归评估器
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")

    val rmse = evaluator.evaluate(frame)

    println("均方根误差为：" + rmse)

    // 为每个用户生成前十热门的电影推荐
    val movieRecs = model.recommendForAllItems(10)
    println("为每个用户生成前十热门的电影推荐")
    movieRecs.show(false)

    // 统计一部电影，偏好程度位于前十用户
    val userRecs = model.recommendForAllUsers(10)
    println("统计一部电影，偏好程度位于前十用户")
    userRecs.show(false)

    // 为一组指定用户生成前十热门的电影推荐
    val user = file.select(als.getUserCol).distinct().limit(3)
    val frame1 = model.recommendForUserSubset(user,10)
    println("为一组指定用户生成前十热门的电影推荐")
    frame1.show(false)

    // 统计一组电影，偏好程度前十的用户
    val item = file.select(als.getItemCol).distinct().limit(3)
    val frame2 = model.recommendForItemSubset(item,10)
    println("统计一组电影，偏好程度前十的用户")
    frame2.show(false)
  }
}
