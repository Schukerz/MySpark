package com.atguigu.spark.day03_test

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark03_Transmission {
  def main(args: Array[String]): Unit = {
val conf: SparkConf = new SparkConf().setAppName("Transmission").setMaster("local[2]")
  .registerKryoClasses(Array(classOf[Seacher]))
    val sc = new SparkContext(conf)
    val rdd: RDD[String] = sc.parallelize(List("hello key","hello scala","hello spark"))
    val seacher: Seacher = new Seacher("hello")
    val rdd2: RDD[String] = seacher.search(rdd)
    rdd2.collect().foreach(println)
    sc.stop()
  }
}
class Seacher(val query:String) extends Serializable {
  def isMatch(s:String)={
    s.contains(query)
  }
  def search(rdd:RDD[String])={
    rdd.filter(isMatch)
  }
def search2(rdd:RDD[String])={
  val q: String = this.query
  rdd.filter(s=>s.contains(q))
}
}