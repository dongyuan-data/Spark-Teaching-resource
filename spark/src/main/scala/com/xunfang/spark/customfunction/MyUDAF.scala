package com.xunfang.spark.customfunction

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}

//本类自定义UDAF函数是为了计算平均值
object MyUDAF{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MyUDAF").master("local[2]").getOrCreate()
    spark.udf.register("myAvg",new myAvg)
    spark.read.format("jdbc")
      .option("url","jdbc:mysql://master1:3306/spark")
      .option("user","root").option("password","123456")
      .option("dbtable","sparkudaf")
      .option("driver","com.mysql.cj.jdbc.Driver")
      .load().createOrReplaceTempView("people")
    println("原数据为：")
    spark.sql("select * from people").show
    println("使用UDAF函数后的数据为：")
    spark.sql("select myAvg(score) from people").show
    spark.stop()
  }
}
class myAvg extends UserDefinedAggregateFunction{
  /**
   * @return : 确定聚合函数输入参数的数据类型
   */
  /*
  StructField :结构为
  case class StructField(
  name : String   #字段的名称,名字可以随意设置相当于标识,使用者可以根据名称来选择对应参数
  dataType : DataType   #字段的数据类型
  nullable : Boolean = true   #字段的值是否可以为空默认值为true
  metadata : Metadata = Metadata.empty    #字段的元数据
  ){}
  StrucType : 由一个或多个StructField构成  可以用StrucType(fields:Seq[StrucType])的方式创建对象
  下列代码的意思就是说输入列中数据类型为Double类型
   */
  override def inputSchema: StructType = {StructType(StructField("inputColumn",DoubleType) :: Nil)}
  /*
  上述代码也可以写成
  override def inputSchema: StructType = {new StructType().add("inputCoulumn",DoubleType)}
   */


  /**
   * @return : 确定缓冲区中值的类型
   */
  /*
  缓冲区就是计算过程中数据存放的位置,我们计算的过程一共需要两个值,
  一个是所有数据的总和,一个是所有数据的数量.本行代码就是规定求和的数据类型和数量的数据类型
   */
  override def bufferSchema: StructType = StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)
  /*
  还可以写成
  override def bufferSchema: StructType = {new StructType().add("sum", LongType).add("count", LongType)}
  */
  /**
   * @return : 确定最终返回值的类型
   */
  /*
   本行代码用来规定自定义UDAF函数的最终返回值类型
   */
  override def dataType: DataType = DoubleType

  /**
   * @return : 当输入的数据相同时返回相同的输出数据
   */
  /*
  如果输入数据不变那就不重新运算直接输出上次运算的结果
   */
  override def deterministic: Boolean = true


  /**
   * 初始化
   * @param buffer : 代表聚合缓冲区的行
   */
  /*
  将聚合运算中间的两个数据sum和count进行初始化
   */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //储存数据的总和
    buffer(0) = 0d
    //储存数据的个数
    buffer(1) = 0L
  }

  /**
   * 相同Excutor间的合并
   * @param buffer
   * @param input
   */
  /*
  input代表的就是需要进行求平均值的列中的一行数据
  本行代码实质上是将相同执行节点中每一行不为空的的数据进行合并
   */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getDouble(0) + input.getDouble(0)
      //1L后的L不能省略,省略会报错java中Long类型不能转换为Integer类型
      buffer(1) = buffer.getLong(1) + 1L
    }
  }

  /**
   * 不同Executor间的合并
   * @param buffer1
   * @param buffer2
   */
  /*
  当输入的行内容不为空时,将运算中生成的sum和count进行合并,将其存到MutalbeAggregationBuffer中
   */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    if(!buffer2.isNullAt(0)){
      buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }
  }

  /**
   * 计算最终结果
   * @param buffer :
   * @return 返回结果为最终计算结果返回值类型为Double类型
   */
  /*
  将所有数据的总和除以所有数据的个数得到最终结果
  注释掉的内容是用来在idea中测试使用的本次实验直接将其打成jar包导入虚拟机中使用所以可加可不加
   */
  override def evaluate(buffer: Row): Double = {
    println(buffer.getDouble(0),buffer.getLong(1))
    //.toDouble也是不可以省略的否则会报错,java中的Long类型不能转换成Double类型
    buffer.getDouble(0)/buffer.getLong(1).toDouble
  }

}
