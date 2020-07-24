package com.xunfang.spark.ml.feature

import java.util._

import org.apache.spark.ml.attribute.{Attribute, AttributeGroup, NumericAttribute}
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

object VectorSlicerTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
        .appName("VectorSlicerTest")
        .master("local[2]")
        .enableHiveSupport().getOrCreate()
    val data = Arrays.asList(
      Row(Vectors.sparse(3, Seq((0, -2.0), (1, 2.3),(2,2.5)))),
      Row(Vectors.dense(-2.0, 2.3, 0.0)))
    val defaultAttr = NumericAttribute.defaultAttr
    val attributes = Array("f1","f2","f3").map(defaultAttr.withName)
    val attributeGroup = new AttributeGroup("userFeatures",
      attributes.asInstanceOf[Array[Attribute]])
    val df = spark.createDataFrame(data,StructType(Array(attributeGroup.toStructField())))
    println("查看转化后数据")
    df.show(false)
    val slicer = new VectorSlicer().setInputCol("userFeatures").setOutputCol("features")
    slicer.setNames(Array("f3")).setIndices(Array(1))
    val output = slicer.transform(df)
    println("打印最终结果")
    output.show(false)
  }
}
