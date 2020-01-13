package com.atguigu.sql.day06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DF2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DF2RDD")
      .getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("X:\\spark\\employees.json")
    val ds: Dataset[User2] = df.as[User2]
    val df2: DataFrame = ds.toDF()
    df2.show()
    spark.stop()
  }
}
case class User2(name:String,salary:Long)