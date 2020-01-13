package com.atguigu.sql.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SparkSession, types}

object Rdd2Df2 {
  def main(args: Array[String]): Unit = {

    //入口:sparkSession
    val builder: SparkSession.Builder = SparkSession.builder().master("local[*]").appName("hello")
    val session: SparkSession = builder.getOrCreate()
    //创建df
   val list = List(User1("Jack",20),User1("Tom",19))
     val rdd: RDD[Row] = session.sparkContext.parallelize(list).map(u=>Row(u.name,u.age.toString))

    val structType = types.StructType(List(StructField("name",StringType),StructField("age",StringType)))
    val df: DataFrame = session.createDataFrame(rdd,structType)

//    session.create
    //查询df
    df.show(100)
    //关闭session
    session.stop()
  }
}
case class User1(name:String,age:Int)
