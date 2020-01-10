package com.atguigu.spark.day03_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark05_Partitioner {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Spark05_Partitioner").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val list = List(30,40,50,10,70,60,10,20)
        val rdd: RDD[Int] = sc.parallelize(list,2)
        val rdd1: RDD[(Int, Int)] = rdd.map((_,1))
    val rdd2: RDD[(Int, Int)] = rdd1.partitionBy(new MyPartitioner(3))
    val rdd3: RDD[Array[(Int, Int)]] = rdd2.glom()
    rdd3.collect().foreach(a=>println(a.mkString(",")))
    sc.stop()
  }
}
class MyPartitioner(val partitionNum:Int) extends Partitioner{
  override def numPartitions: Int =partitionNum

  override def getPartition(key: Any): Int = key match{
    case null => 0
    case _ => key.hashCode().abs % partitionNum
  }

  override def hashCode(): Int = partitionNum

  override def equals(obj: Any): Boolean = obj match{
    case p: MyPartitioner => p.numPartitions == numPartitions
    case _ => false

  }
}
