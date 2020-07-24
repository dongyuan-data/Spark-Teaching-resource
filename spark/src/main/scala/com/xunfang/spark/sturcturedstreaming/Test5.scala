package com.xunfang.spark.sturcturedstreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession

object Test5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Test5").master("local[2]").getOrCreate()
    val frame = spark.readStream.format("socket")
      .option("host", "master1").option("port", "10000").option("includeTimestamp", "true").load()
    import spark.implicits._
    val map = frame.as[(String, Timestamp)].flatMap {
      case (word, ts) => word.split(" ").map((_, ts))
    }.toDF("word","ts")
    import org.apache.spark.sql.functions._
    val frame1 = map.groupBy(
      window($"ts", "1 minutes", "1 minutes"), $"word"
    ).count()
    frame1.writeStream.format("console").outputMode("update")
      .option("truncate",false).start().awaitTermination()
  }
}
