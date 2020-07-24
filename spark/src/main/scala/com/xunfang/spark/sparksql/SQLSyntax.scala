package com.xunfang.spark.sparksql

import org.apache.spark.sql.SparkSession

object SQLSyntax {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SQLSyntax").master("local[2]").getOrCreate()
    val sc = spark.sparkContext



    import spark.implicits._
    val ds1 = Seq(person("lilei",19,"man"),person("hanmeimei",15,"women"),person("zhangsan",30,"man")).toDS

    println("创建临时视图")
    ds1.createOrReplaceTempView("person")

    println("查询所有数据")
    spark.sql("select * from person").show

    println("查找年龄在20岁以下的人")
    spark.sql("select name,age,sex from person where age<20 ").show

    println("查询表中有多少人")
    spark.sql("select count(sex) from person").show

    println("创建全局视图进行数据查询")

    val df = sc.parallelize(Array(("yuwen", "zhangsan", 80), ("yuwen", "lisi", 90), ("shuxue", "zhangsan", 90), ("shuxue", "lisi", 95))).toDF("course", "name", "score")

    df.createGlobalTempView("people")

    println("使用全路径表名查询数据")
    spark.sql("select * from global_temp.people").show

    println("只查看zhangsan的成绩")
    spark.sql("select * from global_temp.people where name='zhangsan' ").show

    println("查询张三的总成绩")
    spark.sql("select first(name),sum(score) from global_temp.people where name='zhangsan' ").show

    println("在新的sparksession中查看数据")
    spark.newSession.sql("select * from global_temp.people").show
  }
}
case class person(name:String,age:Int,sex:String)