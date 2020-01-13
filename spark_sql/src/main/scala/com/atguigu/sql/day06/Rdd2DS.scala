package com.atguigu.sql.day06

import org.apache.spark.sql.{Dataset, SparkSession}

object Rdd2DS {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("CreateDS")
      .getOrCreate()

    import spark.implicits._
    val persons: Seq[Person1] = Seq(Person1("lisi",20),Person1("zhangsan",19))
    val DS: Dataset[Person1] = persons.toDS()
    DS.createOrReplaceTempView("p")
    spark.sql("select * from p").show()
    println("_______________")

    DS.show()
    println("+++++++++++++++++++++")
    DS.select("age").show()
    println("++++++++++++++")
    DS.map(m => m.name).show()
    spark.close()
  }
}
case class Person1(name:String,age:Int)