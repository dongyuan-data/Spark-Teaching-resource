package com.xunfang.spark.customfunction

import org.apache.spark.sql.SparkSession


object MyUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MyUDF").master("local[2]").getOrCreate()
    spark.read.json("hdfs://master1:8020/data/sql.json").createOrReplaceTempView("people")
    println("自定义UDF函数程序演示")
    println("原始数据")
    spark.sql("select * from people").show
    spark.udf.register("getName",(s:String)=>{val string = s.split(",");string(0)})
    println("实验获取名字的UDF函数是否可以使用")
    spark.sql("select id,getName(userInfo) from people").show
    spark.udf.register("getAge",(s:String)=>{val string = s.split(",");string(1).toInt})
    spark.udf.register("getSex",(s:String)=>{val string = s.split(",");string(2)})
    val df1 = spark.sql("select id,getName(userInfo) `name`,getAge(userInfo) `age `,getSex(userInfo) `sex` from people")
    println("查看结果")
    df1.show
    println("查看表结构")
    df1.printSchema
  }
}
