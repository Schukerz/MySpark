package com.atguigu.sql.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Rdd2Df {
  def main(args: Array[String]): Unit = {

    //入口:sparkSession
    val builder: SparkSession.Builder = SparkSession.builder().master("local[*]").appName("hello")
    val session: SparkSession = builder.getOrCreate()
    import session.implicits._
    //创建df
   val list = List(User("Jack",20),User("Tom",19))
     val rdd: RDD[User] = session.sparkContext.parallelize(list)

//    session.create
    val df = rdd.toDF("name","age")
    //查询df
    df.show()
    //关闭session
    session.close()
  }
}
case class User(name:String,age:Int)
