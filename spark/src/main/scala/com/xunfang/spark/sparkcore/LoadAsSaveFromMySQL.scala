package com.xunfang.spark.sparkcore

import org.apache.spark.sql.SparkSession

object LoadAsSaveFromMySQL {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("LoadAsSaveFromMySQL").master("local[2]").getOrCreate()
    val jdbc = spark.read.format("jdbc")
      .option("url","jdbc:mysql://master1:3306/spark")
      .option("user","root").option("password","123456")
      .option("dbtable","sparksql")
      .option("driver","com.mysql.cj.jdbc.Driver").load()
    println("查看mysql中的数据")
    jdbc.show

    println("将导入的数据保存到mysql中并且作为对原表的追加内容")
    jdbc.write.format("jdbc").mode("append")
      .option("url","jdbc:mysql://master1:3306/spark")
      .option("user","root").option("password","123456")
      .option("dbtable","sparksql").option("driver","com.mysql.cj.jdbc.Driver").save()

    println("读取保存后的数据")
    val jdbc1 = spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://master1:3306/spark",
        "user"->"root","password"->"123456",
        "dbtable"->"sparksql",
        "driver"->"com.mysql.cj.jdbc.Driver")).load()
    jdbc1.show

    import java.util.Properties
    val props:Properties = new Properties()

    //将mysql的用户导入配置中
    props.setProperty("user","root")

    props.setProperty("driver","com.mysql.cj.jdbc.Driver")

    //将用户对应密码导入配置中
    props.setProperty("password","123456")

    //连接读取数据
    val jdbc2 = spark.read.jdbc("jdbc:mysql://master1:3306/spark","sparksql",props)

    println("读取mysql中sparksql表中的数据")
    jdbc2.show

    //创建一个临时视图
    jdbc2.createOrReplaceTempView("hello")

    //向df中添加数据
    val df1 = spark.sql("insert into hello values('xiaohong',17,'women')")

    //查询数据
    val df2 = spark.sql("select * from hello")

    println("将读取的数据创建一个临时视图，并添加数据打印")
    df2.show

    println("读取mysql中sparkshell表的数据")
    spark.read.jdbc("jdbc:mysql://master1:3306/spark","sparkshell",props).show

    //将数据保存至sparkshell表中对sparkshell表中数据进行覆盖操作
    df2.write.mode("overwrite").jdbc("jdbc:mysql://master1:3306/spark","sparkshell",props)

    println("将数据保存至sparkshell表中对sparkshell表中数据进行覆盖操作显示结果")
    spark.read.jdbc("jdbc:mysql://master1:3306/spark","sparkshell",props).show
    spark.stop()
  }
}
