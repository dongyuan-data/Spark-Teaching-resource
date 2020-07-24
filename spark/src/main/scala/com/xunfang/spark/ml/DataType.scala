package com.xunfang.spark.ml

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.Matrices
import org.apache.spark.sql.SparkSession

object DataType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    println("-------------------------------------------------------------------")
    println("本地向量")
    import org.apache.spark.ml.linalg.Vectors
    println("创建一个稠密本地向量")
    val dv1 = Vectors.dense(1.0,0.0,3.0)
    println(dv1.toString)
    println("创建一个稀疏本地向量")
    val sv1 = Vectors.sparse(3,Array(0,2),Array(1.0,3.0))
    println(sv1.toString)
    println("创建一个稀疏本地向量,Seq()内的数据(0,2.0)表示,0号索引位置为非零数2.0,(2,3.0)表示,2号索引位置为非零数3.0")
    val sv2 = Vectors.sparse(3,Seq((0,2.0),(2,3.0)))
    println(sv2.toString)
    println("使用稠密向量创建稀疏向量")
    val sv3 = dv1.toSparse
    println(sv3.toString())
    println("使用稀疏向量创建稠密向量")
    val dv2 = println(sv2.toDense.toString())
    println("-------------------------------------------------------------------")
    println("标注点")
    println("创建一个标签为1.0的稠密向量标注点(视为正样本)")
    val lp1 = LabeledPoint(1.0,Vectors.dense(1.0,0.0,3.0))
    println(lp1.toString())
    println("创建一个标签为0.0的稀疏向量标注点(视为负样本)")
    val lp2 = LabeledPoint(0.0,Vectors.sparse(3,Array(0,2),Array(1.0,3.0)))
    println(lp2.toString())
    println("读取一个libsvm格式文件数据")
    val df = spark.read.format("libsvm").option("numFeatures","692").load("hdfs://master1:8020/sparkdata/sample_libsvm_data.txt")
    df.show()
    println("-------------------------------------------------------------------")
    println("本地矩阵")
    println("创建一个稠密矩阵")
    val md1 = Matrices.dense(3,2,Array(1.0,3.0,5.0,2.0,4.0,6.0))
    println(md1.toString())
    println("创建一个稀疏矩阵")
    val ms1 =Matrices.sparse(3,2,Array(0,1,3),Array(0,2,1),Array(9,7,8))
    println(ms1.toString())
    println("直接看稀疏向量不好解释参数转换成稠密向量可以更直观反映参数的作用")
    println(ms1.toDense.toString())
  }
}
