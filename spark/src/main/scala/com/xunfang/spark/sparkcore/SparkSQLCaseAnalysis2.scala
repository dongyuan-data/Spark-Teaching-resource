package com.xunfang.spark.sparkcore

import org.apache.spark.sql.SparkSession

object SparkSQLCaseAnalysis2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSQLCaseAnalysis2")
      .master("local[2]").enableHiveSupport().getOrCreate()
    println("需求一程序演示")
    val df1 = spark.read.csv("hdfs://master1:8020/data/sql2.csv").
      toDF("date","country","countryCode","province","provinceCode","city",
        "cityCode","confirmed","suspected","cured","dead")

    import org.apache.spark.sql.types.LongType

    val df2 = df1.select(df1("date" ),df1("country"),df1("countryCode"),df1("province"),
      df1("provinceCode"),df1("city"),df1("cityCode"),df1("confirmed").cast(LongType),
      df1("suspected").cast(LongType), df1("cured").cast(LongType), df1("dead").cast(LongType))

    import spark.implicits._

    val ds1 = df2.as[nCoV1]

    ds1.createOrReplaceTempView("nCoV_2_29")

    println("进行数据筛选")
    val df3 = spark.sql("select date,country,confirmed,cured,dead from nCoV_2_29 where province is null")

    df3.show

    df3.createOrReplaceTempView("tmp1")

    println("将数据进行整理")
    spark.sql("select date,sum(confirmed) `confirmed`,sum(cured) `cured`," +
      "sum(dead) `dead` from tmp1 group by date").createOrReplaceTempView("tmp2")

    //自定义函数
    spark.udf.register("toDouble",(s:Long)=>{s.toDouble})

    //通过自定义函数将数据数据类型进行转换
    spark.sql("select date,toDouble(confirmed)`confirmed`,toDouble(cured)`cured`," +
      "toDouble(dead)`dead` from tmp2").createOrReplaceTempView("tmp3")
    spark.sql("select * from tmp3").show

    println("将结果转换为百分率形式显示并保留两位小数")
    val df4 =spark.sql("select date,(round(cured/confirmed*100,2))||'%' `curedPct`," +
      "(round(dead/confirmed*100,2))||'%' `deadPct` from tmp3")

    df4.show
    println("--------------------------------------------------------------------------------")
    println("需求二程序演示")
    val df5 = spark.read.csv("hdfs://master1:8020/data/sql1.csv").toDF("date","country","countryCode","province","provinceCode","city","cityCode","confirmed","suspected","cured","dead")

    val df6 = df5.select(df5("date" ),df5("country"),df5("countryCode"),df5("province"),df5("provinceCode"),df5("city"),df5("cityCode"),df5("confirmed").cast(LongType), df5("suspected").cast(LongType), df5("cured").cast(LongType), df5("dead").cast(LongType))

    val ds2 = df6.as[nCoV1]
    ds2.createOrReplaceTempView("nCoV_2_1")

    //过滤数据
    spark.sql("select date,province,city,country,confirmed,suspected,cured,dead from nCoV_2_1 where city is not null").createOrReplaceTempView("tmp4")

    //过滤数据
    spark.sql("select date,province,city,country,confirmed,suspected,cured,dead from nCoV_2_29 where city is not null").createOrReplaceTempView("tmp5")

    //以2月29日数据为主表left链接2月1日数据,因为2月29日对比与2月1日来说增加多个城市.
    spark.sql("select a.date,a.city,a.province,a.confirmed,a.suspected,a.cured,a.dead,b.date `earlyDate`,b.confirmed`earlyConfirmed`,b.suspected `earlySuspected`,b.cured `earlyCured`,b.dead `earlyDead`  from tmp5 a left join tmp4 b on a.city = b.city").createOrReplaceTempView("tmp6")

    //将2月1日中为null的数据转换为0
    spark.sql("select date,province,city,confirmed,suspected,cured,dead,earlydate,ifnull(earlyConfirmed,0)`earlyconfirmed`,ifnull(earlySuspected,0)`earlysuspected`,ifnull(earlyCured,0)`earlycured`,ifnull(earlyDead,0)`earlydead` from tmp6").createOrReplaceTempView("tmp7")

    //得到增减幅度的数据
    val df7 = spark.sql("select date,province,city,(confirmed - earlyconfirmed)`addconfirmed`,(suspected-earlysuspected)`addsuspected`,(cured-earlycured)`addcured`,(dead-earlydead)`adddead` from tmp7")

    import java.util.Properties

    val props:Properties = new Properties()

    props.setProperty("user","root")

    props.setProperty("password","123456")

    props.setProperty("driver","com.mysql.cj.jdbc.Driver")

    df7.write.mode("overwrite").jdbc("jdbc:mysql://master1:3306/spark","addData",props)
    df7.show
    println("--------------------------------------------------------------------------------")
    println("需求三程序演示")
    df7.createOrReplaceTempView("tmp8")

    //将两表链接得到2月1日数据以及2月1日到2月29日的新增数据
    val df8  = spark.sql("select a.date,a.province,a.city,a.earlyconfirmed,b.addconfirmed from tmp7 a left join tmp8 b on a.city=b.city")

    //因为有很多城市2月1日数据为0但是2月29日有数据,所以新增率为100%,所以需要将新增数据直接赋值给2月1日的数据就可以得到100%的结果.
    df8.map(row => {val l = row.getAs[Long](3) ; val s = if (l == 0) row.getAs[Long](4) else l ;(row(0).toString , row(1).toString, row(2).toString, s, row(4).toString.toLong) }).createOrReplaceTempView("tmp9")

    //得到新增率
    val df9 = spark.sql("select _1 `date`,_2 `province`,_3 `city`,_4 `earlyconfirmed`,_5 `addconfirmed`,(round(_5/_4*100,2)) `addPct` from tmp9 order by addPct desc limit 10")

    df9.show
    println("--------------------------------------------------------------------------------")
    println("需求二补充程序演示")
    spark.sql("select date,province,city,country,confirmed,suspected,cured,dead from nCoV_2_1 where provinceCode='710000' or provinceCode='810000' or provinceCode='820000'").createOrReplaceTempView("tmp10")

    spark.sql("select date,province,city,country,confirmed,suspected,cured,dead from nCoV_2_29 where provinceCode='710000' or provinceCode='810000' or provinceCode='820000'").createOrReplaceTempView("tmp11")

    //以2月29日数据为主表left链接2月1日数据,因为2月29日对比与2月1日来说增加多个城市
    spark.sql("select a.date,a.city,a.province,a.confirmed,a.suspected,a.cured,a.dead,b.date `earlydate`,b.confirmed`earlyconfirmed`,b.suspected `earlysuspected`,b.cured `earlycured`,b.dead `earlydead`  from tmp11 a left join tmp10 b on a.province = b.province").createOrReplaceTempView("tmp12")

    //得到增减幅度的数据
    val df10 = spark.sql("select date,province,city,(confirmed - earlyconfirmed)`addconfirmed`,(suspected-earlysuspected)`addsuspected`,(cured-earlycured)`addcured`,(dead-earlydead)`adddead` from tmp12")

    //和df7的数据进行合并
    val df11 = df7.union(df10)
    df11.show
    df11.write.mode("overwrite").jdbc("jdbc:mysql://master1:3306/spark","addData_1",props)
  }
}
case class nCoV1(date: String,country: String,countryCode: String,province: String,provinceCode: String,city: String,cityCode: String,confirmed :Long,suspected:Long,cured:Long,dead:Long)