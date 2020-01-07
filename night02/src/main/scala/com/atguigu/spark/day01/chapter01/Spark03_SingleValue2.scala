package com.atguigu.spark.day01.chapter01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_SingleValue2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Single2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val list1 = List(1,2,3,4,5,3,2,2,"aa")
    val rdd: RDD[Any] = sc.parallelize(list1,2)
//    val rdd2: RDD[Int] = rdd.distinct()
//    rdd2.collect().foreach(println)
//    println(rdd2.collect.mkString(","))
//    sc.stop()

    //distinct
//      val users = List(new User(1,"a"),User(1,"b"),User(2,"c"))
//      val rdd1: RDD[User] = sc.parallelize(users,2)
//    val rdd2: RDD[User] = rdd1.distinct
//val rdd2: RDD[Int] = rdd.filter(s=>s>2)
    val rdd2: RDD[Int] = rdd.collect {
      case i: Int  if i > 2 => i + 10
    }
    rdd2.collect.foreach(println)
    sc.stop()
  }

}
case class User(age:Int,name:String){
  override def hashCode(): Int = this.age
  override def equals(obj: Any): Boolean = obj.asInstanceOf[User].age == this.age
}