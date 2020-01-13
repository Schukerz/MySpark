package com.atguigu.sql.day07.hive

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveDemo {
  def main(args: Array[String]): Unit = {
    import ss.sql
    System.setProperty("HADOOP_USER_NAME","atguigu")
    val ss: SparkSession = SparkSession.builder()
      .appName("HiveDemo")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    sql("use test1")
    //sql("select * from user").show
    import ss.implicits._
    val df: DataFrame = Seq(("wangwu",5),("zhuangliu",6)).toDF("name","id")
    df.write.saveAsTable("user1")
    println("111111111111111")
    sql("select * from user1").show
    println("222222222222222")
    //df.write.mode("overwrite").saveAsTable("user1")
    sql("select * from user1").show
  }

}
