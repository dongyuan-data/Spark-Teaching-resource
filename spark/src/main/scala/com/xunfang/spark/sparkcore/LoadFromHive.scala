package com.xunfang.spark.sparkcore

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.spark.sql.SparkSession

object LoadFromHive {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadFromHive").master("local[2]").enableHiveSupport().getOrCreate()

    println("查看hive中的数据库")
    spark.sql("show databases").show
    println("使用leran数据库")
    spark.sql("use learn").show
    println("查看learn数据库中的表")
    spark.sql("show tables").show
    println("查看person表中的数据")
    spark.sql("select * from person").show
    println("对person表中的age列进行求和" )
    spark.sql("select sum(age) from person").show
    println("person中的name列根据age列进行排序")
    spark.sql("select name,rank()over(order by age) from person").show


    spark.stop()
  }
}
