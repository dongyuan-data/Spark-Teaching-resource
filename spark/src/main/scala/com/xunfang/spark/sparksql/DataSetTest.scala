package com.xunfang.spark.sparksql

import org.apache.spark.sql.SparkSession

object DataSetTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataSetTest").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    println("使用基本类型序列创建DS")
    import spark.implicits._
    val ds1 = Seq(1,2,3,4,5).toDS
    ds1.show
    ds1.printSchema
    println("使用样例类序列创建DS")


    val ds2 = Seq(person3("lilei",19),person3("hanmeimei",18)).toDS

    ds2.show

    ds2.printSchema

    println("通过RDD创建DS")

    val rdd3 = sc.parallelize(Array(("zhangsan,15,man"),("lisi,18,man")))

    val ds3 = rdd3.map(word =>{ val string = word.split(",");person2(string(0),string(1).toInt,string(2))}).toDS

    ds3.show

  }
}
case class person3(name:String,age:Int)
case class person2(name:String,age:Int,sex:String)