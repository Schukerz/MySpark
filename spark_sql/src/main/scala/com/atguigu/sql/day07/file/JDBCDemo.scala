package com.atguigu.sql.day07.file

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    //读取jdbc方式1
//    val jdbcDF = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://hadoop103:3306/test")
//      .option("user", "root")
//      .option("password", "root")
//      .option("dbtable", "user")
//      .load()
    //读取jdbc方式2
//    val props = new Properties()
//    props.setProperty("user","root")
//    props.setProperty("password","root")
//    val jdbcDF: DataFrame = spark.read.jdbc("jdbc:mysql://hadoop103:3306/test","user",props)
//    jdbcDF.show

    //写入jdbc
    import spark.implicits._
    val rdd: RDD[User] = spark.sparkContext.makeRDD(Seq(User("Stans",30),User("Mathilda",12),User("Leon",30)))
    val ds: Dataset[User] = rdd.toDS
    ds.write
      .format("jdbc")
      .option("url","jdbc:mysql://hadoop103:3306/test")
      .option("user","root")
      .option("password","root" )
      .option("dbtable","user")
      .mode("overwrite")
      .save()

    val props = new Properties()
    props.setProperty("user","root")
    props.setProperty("password","root")
//    ds.write.jdbc("jdbc:mysql://hadoop103:3306/test","user",props)

    spark.read.jdbc("jdbc:mysql://hadoop103:3306/test","user",props).show()

  }

}
case class User(name:String,age:Int)