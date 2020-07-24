package com.xunfang.spark.sturcturedstreaming

import java.sql.Timestamp
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Test6 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test6").master("local[2]").getOrCreate()
    val frame = spark.readStream.format("socket").option("host", "master1").option("port", "10000")
      .option("includeTimestamp", "true").load()
    import spark.implicits._
    val map = frame.as[(String, Timestamp)].flatMap {
      case (word, timestamp) => word.split(" ").map((_, timestamp))
    }.toDF("word","timestamp")

    val value1 = map.withWatermark("timestamp", "1 minutes")
      .groupBy(
        window($"timestamp", "1 minutes", "1 minutes"), $"word"
      ).count()
    value1.writeStream.format("console").outputMode("append").option("truncate",false)
      .start().awaitTermination()
  }
}
