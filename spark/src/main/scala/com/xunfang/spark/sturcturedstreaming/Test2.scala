package com.xunfang.spark.sturcturedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Test2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Test2")
      .getOrCreate()
    val schema = new StructType().add("date", "String").add("country", "String")
      .add("countryCode", "String").add("province", "String")
      .add("provinceCode", "String").add("city", "String")
      .add("cityCode", "String").add("confirmed", "Int")
      .add("suspected", "Int").add("cured", "Int").add("dead", "Int")
    val lines = spark.readStream
      .format("csv")
      .schema(schema)
      .csv("hdfs://master1:8020/ss/")
    val query = lines
      .writeStream
      .outputMode("update")
      .format("console")
      .start()
    query.awaitTermination()
  }
}