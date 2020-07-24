package com.xunfang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDTest").setMaster("local[2]")
    val sc = new SparkContext(conf)

    println("利用集合创建RDD")
    val rdd1 = sc.parallelize(Array(1,2,3,4,5,6,7,8))
    print("rdd1为：")
    rdd1.collect().foreach(print)
    println("\nrrd1的分区为：" + rdd1.partitions.size)

    val rdd2 = sc.parallelize(1 to 8)
    print("rdd2为：")
    rdd2.collect().foreach(print)
    println("\nrrd2的分区为：" + rdd2.partitions.size)

    val rdd3 = sc.parallelize(List(1,2,3,4,5,6,7,8))
    print("rdd3为：")
    rdd3.collect().foreach(print)
    println("\nrrd3的分区为：" + rdd3.partitions.size)

    val rdd4 = sc.makeRDD(Array(1,2,3,4,5,6,7,8))
    print("rdd4为：")
    rdd4.collect().foreach(print)
    println("\nrrd4的分区为：" + rdd4.partitions.size)

    val rdd5 = sc.makeRDD(1 to 8)
    print("rdd5为：")
    rdd5.collect().foreach(print)
    println("\nrrd5的分区为：" + rdd5.partitions.size)

    val rdd6 = sc.makeRDD(List(1,2,3,4,5,6,7,8))
    print("rdd6为：")
    rdd6.collect().foreach(print)
    println("\nrrd6的分区为：" + rdd6.partitions.size)

    println("从外部存储创建RDD")
    val rdd7 = sc.textFile("hdfs://master1:8020/user/root/data/rdd1")
    print("rdd7为：")
    rdd7.collect.foreach(print)
    println("\nrrd7的分区为：" + rdd7.partitions.size)

    println("使用parallelize和makeRDD创建RDD时增减分区数")
    val rdd8 = sc.parallelize(Array(1,2,3,4,5,6,7,8),3)
    println("rrd8的分区为：" + rdd8.partitions.size)
    val rdd9 = sc.parallelize(1 to 8,1)
    println("rrd9的分区为：" + rdd9.partitions.size)
    val rdd10 = sc.makeRDD(Array(1,2,3,4,5,6,7,8),1)
    println("rrd10的分区为：" + rdd10.partitions.size)
    val rdd11 = sc.makeRDD(1 to 8,3)
    println("rrd11的分区为：" + rdd11.partitions.size)

    println("从外部存储创建RDD时多行文本的影响")
    val rdd12 = sc.textFile("hdfs://master1:8020/user/root/data/rdd2")
    print("rdd12为：\n")
    rdd12.collect.foreach(println)
    println("rrd12的分区为：" + rdd12.partitions.size)
    println("rdd12是从外部存储创建时使用三行文本")
    println("rdd12中元素的个数" + rdd12.count)
    println("rdd7是从外部存储创建时使用单行文本")
    println("rdd7中元素的个数" + rdd7.count)

    println("从外部存储创建RDD时增减分区数")
    val rdd13 = sc.textFile("hdfs://master1:8020/user/root/data/rdd2",1)
    println("rdd13的分区数为：" + rdd13.partitions.size)
    val rdd14 = sc.textFile("hdfs://master1:8020/user/root/data/rdd2",3)
    println("rdd14的分区数为：" + rdd14.partitions.size)
  }
}
