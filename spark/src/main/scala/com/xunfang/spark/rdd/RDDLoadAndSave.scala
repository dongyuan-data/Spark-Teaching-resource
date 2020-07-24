package com.xunfang.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}
//import scala.util.parsing.json.JSON

object RDDLoadAndSave {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDLoadAndSave").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /*val rdd = sc.textFile("file:///root/data/t1.txt")
    println("读取t1的文件生成rdd")
    rdd.foreach(println)*/

    /*val rdd1 = sc.parallelize(1 to 8)
    rdd1.saveAsTextFile("file:///root/data/output")*/
    println("将rdd保存成text文件保存到本地文件系统中 可使用cat /root/data/output/* 查看")


    /*val rdd2 = sc.textFile("file:///root/data/json.json")
    val rdd3 = rdd2.map(JSON.parseFull)
    println("读取json文件生成rdd3")
    rdd3.foreach(println)*/

   /* val rdd4 = sc.parallelize(Array((1,2),(2,4),(3,6)))
    rdd4.saveAsSequenceFile("file:///root/data/output1")*/
    println("将rdd保存成二进制文件保存到本地文件系统中 可使用cat /root/data/output1/* 命令查看")

    /*val rdd5 = sc.sequenceFile[Int,Int]("file:///root/data/output1")
    println("读取二进制文件生成rdd")
    rdd5.foreach(println)*/

    val rdd6 = sc.textFile("hdfs://master1:8020/data/t1.txt")
    println("读取hdfs上的text文件生成rdd")
    rdd6.foreach(println)

    val rdd7 = sc.parallelize(1 to 8)
    rdd7.saveAsTextFile("hdfs://master1:8020/output/text")
    println("将rdd保存成text文本写入到hdfs上 可使用hadoop fs -cat /output/text/* 命令查看")

    val rdd8 = sc.parallelize(Array(("abc",1),("spark",2),("xunfang",3)))
    rdd8.saveAsSequenceFile("hdfs://master1:8020/output/sequence")
    println("将rdd保存成二进制文件写入hdfs上")

    val rdd9 = sc.sequenceFile[String,Int]("hdfs://master1:8020/output/sequence")
    println("读取hdfs上的二进制文件生成rdd 可使用hadoop fs -cat /output/sequence/* 命令查看")
    rdd9.foreach(println)

    /*val rdd10 = sc.textFile("hdfs://master1:8020/data/json.json")
    val rdd11 = rdd10.map(JSON.parseFull)
    println("读取hdfs上的json文件生成rdd")
    rdd11.foreach(println)*/
  }
}
