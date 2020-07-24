package com.xunfang.spark.sturcturedstreaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

object Test4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .master("local[2]")
      .appName("Test4")
      .getOrCreate()
    val schema = new StructType().add("date", "String").add("country", "String")
      .add("countryCode", "String").add("province", "String")
      .add("provinceCode", "String").add("city", "String")
      .add("cityCode", "String").add("confirmed", "Int")
      .add("suspected", "Int").add("cured", "Int").add("dead", "Int")
    import spark.implicits._
    val lines = spark.readStream
      .format("csv")
      .schema(schema)
      .csv("hdfs://master1:8020/ss/*")
      .as[nCoV]
    val value = lines.map(a => {
      //对时间属性进行切分
      val strings = a.date.split("-")
      //转换成Time类型DS
      Time(strings(0).toInt,strings(1).toInt,strings(2).toInt,a.country,a.countryCode,a.province,a.provinceCode,a.city,a.cityCode,a.confirmed,a.suspected,a.cured,a.dead)
    }).createOrReplaceTempView("people")
    //根据时间进行筛选
    val frame = spark.sql("select * from people where day = 1")
    frame.writeStream
      .outputMode("append")
      .format("console")
      .start()
      .awaitTermination()
  }
}
case class nCoV(date:String,country:String,countryCode:String,province:String,provinceCode:String,city:String,cityCode:String,confirmed:Int,suspected:Int,cured:Int,dead:Int)
case class Time(year:Int,month:Int,day:Int,country:String,countryCode:String,province:String,provinceCode:String,city:String,cityCode:String,confirmed:Int,suspected:Int,cured:Int,dead:Int)