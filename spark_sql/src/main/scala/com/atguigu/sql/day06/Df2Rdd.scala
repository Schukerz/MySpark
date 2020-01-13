package com.atguigu.sql.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Df2Rdd {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DF2RDD")
      .getOrCreate()
    val df: DataFrame = spark.read.json("X:\\spark\\employees.json")
    val rdd: RDD[Row] = df.rdd
    val rdd2: RDD[(String, Long)] = rdd.map(r => {
      (r.getString(0), r.getLong(1))
    })

    rdd2.collect().foreach(println)
    spark.stop()
  }
}
