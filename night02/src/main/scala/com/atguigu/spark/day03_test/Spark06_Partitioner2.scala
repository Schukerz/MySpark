package com.atguigu.spark.day03_test

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark06_Partitioner2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("partitioner").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,5,3,2,5,6,4),2)
    val rdd2: RDD[(Int, Int)] = rdd.map((_,1))
    val rdd3: RDD[(Int, Int)] = rdd2.partitionBy(new MyPartitioner(3))
    val rdd4: RDD[Array[(Int, Int)]] = rdd3.glom()
    rdd4.collect().foreach(a=>println(a.mkString(",")))
    sc.stop()
  }
}
class MyPartitioner(val partitioners : Int) extends Partitioner{
  override def numPartitions: Int = partitioners

  override def getPartition(key: Any): Int = key match{
    case null => 0
    case _ => key.hashCode().abs % numPartitions
  }

  override def hashCode(): Int = numPartitions

  override def equals(obj: Any): Boolean = obj match{
    case p:MyPartitioner => p.numPartitions == numPartitions
    case _ => false

  }
}
