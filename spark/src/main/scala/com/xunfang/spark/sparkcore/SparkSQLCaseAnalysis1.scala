package com.xunfang.spark.sparkcore

import org.apache.spark.sql.SparkSession

object SparkSQLCaseAnalysis1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder().appName("SparkSQLCaseAnalysis1")
      .master("local[2]").enableHiveSupport().getOrCreate()

    println("需求一程序演示")
    val df = spark.read.format("csv")
      .load("hdfs://master1:/data/sparksql")
      .toDF("date","country","countryCode",
        "province","provinceCode","city","cityCode","confirmed","suspected","cured","dead")
    println("查看导入数据")
    df.show
    println("查看导入数据中中每列的类型")
    df.printSchema
    import org.apache.spark.sql.types.LongType
    val df1 = df
      .select(df("date" ),df("country"),df("countryCode"),df("province")
        ,df("provinceCode")cast(LongType),df("city"),df("cityCode")cast(LongType)
          ,df("confirmed").cast(LongType), df("suspected")
          .cast(LongType), df("cured").cast(LongType), df("dead").cast(LongType))
    println("将需要的类型转换为LongType并打印表结构")
    df1.printSchema()

    import spark.implicits._
    val ds = df1.as[nCoV]
    ds.createOrReplaceTempView("nCoV")
    val ds1 = spark.sql("select * from nCoV where city is null")
    println("将所有带有城市信息的数据筛选掉")
    ds1.show
    ds1.createOrReplaceTempView("tmp1")
    println("将所有国家汇总信息都筛选掉")
    val ds2 = spark.sql("select * from tmp1 where tmp1.province is not null ")
    ds2.createOrReplaceTempView("tmp2")
    val ds3 = spark.sql("select * from tmp2")
    ds3.show
    import java.util.Properties
    val props:Properties = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","123456")
    props.setProperty("driver","com.mysql.cj.jdbc.Driver")
    println("将结果保存至mysql中spark库的province_data表内")
    ds3.write.mode("overwrite").
      jdbc("jdbc:mysql://master1:3306/spark","province_data",props)

    println("--------------------------------------------------------------------------------")
    println("需求二程序演示")
    val ds4 = spark.sql("select province," +
      "confirmed from tmp2 order by confirmed desc limit 3")
    ds4.show
    println("--------------------------------------------------------------------------------")
    println("需求三程序演示")
    println("过滤数据")
    val ds7 = spark.sql("select * from nCoV where city is not null")
    ds7.show
    println("抽取台湾省,香港澳门行政区数据")
    val ds8 = spark.sql("select * from nCoV where provinceCode=710000 " +
      "or provinceCode=810000 or provinceCode=820000")
    ds8.show
    println("将上面两个数据进行合并")
    val ds9 = ds7.union(ds8)
    ds9.show
    ds9.createOrReplaceTempView("tmp3")
    spark.sql("select date,province,city,confirmed,suspected,cured," +
      "dead,rank()over(partition by province order by confirmed desc) `rank` from tmp3 ")
      .createOrReplaceTempView("tmp4")
    val ds10 = spark.sql("select * from tmp4 where rank <=3")
    println("筛选每个省份排名前三的城市")
    ds10.show
    println("将数据输出到mysql中,用于查看")
    ds10.write.mode("overwrite").jdbc("jdbc:mysql://master1:3306/spark","city_data",props)
  }
}
case class nCoV(date: String,country: String,countryCode: String,province: String,
                provinceCode: Long,city: String,cityCode: Long,confirmed :Long,
                suspected:Long,cured:Long,dead:Long)