package com.atguigu.sql.day07.file

import org.apache.spark.sql.{DataFrame, SparkSession}

object TestRead {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder()
      .appName("TestRead")
      .master("local[*]")
      .getOrCreate()
    val df: DataFrame = ss.read.format("json").load("X:\\spark\\employees.json")



  }
}
