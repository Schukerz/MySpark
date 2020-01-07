package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag

object Spark06_SingleValue5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("single6").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list = List("aaa","bas","world","hello")
    val rdd: RDD[String] = sc.parallelize(list,2)
//    val rdd2: RDD[String] = rdd.sortBy(s=>(s.length,s))
    val rdd2: RDD[String] = rdd.sortBy(s=>(s.length,s))(Ordering.Tuple2(Ordering.Int.reverse,Ordering.String),ClassTag(classOf[(Int,String)]))
    rdd2.collect.foreach(println)
    sc.stop()
  }

}
