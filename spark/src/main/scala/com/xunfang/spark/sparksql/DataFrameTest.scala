package com.xunfang.spark.sparksql

import org.apache.spark.sql.SparkSession

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameTest").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val df1 = spark.read.json("hdfs://master1:8020/data/json")
    println("使用数据源创建DF")
    df1.show

    println("打印DF的Schema信息")
    df1.printSchema

    println("使用toDF方式创建DF")
    import spark.implicits._
    val df2 = sc.parallelize(Array(1,2,3,4,5)).toDF
    df2.show

    println("使用样例类的方式创建DF")
    val rdd1 = sc.parallelize(Array(("zhangsan",20),("lisi",25)))
    val df3= rdd1.map(word =>{person1(word._1,word._2)})
    df3.toDF.show


  }
}
case class person1(name:String,age:Int)