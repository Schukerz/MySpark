package com.atguigu.spark.day04

//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Result
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Hbase {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Spark01_Hbase").setMaster("local[2]")
//        val sc = new SparkContext(conf)
//        val configuration: Configuration = HBaseConfiguration.create()
//        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
//        configuration.set(TableInputFormat.INPUT_TABLE, "student")
//
//        val rdd: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(configuration,classOf[TableInputFormat],classOf[ImmutableBytesWritable],classOf[Result])
//
//    val rdd2: RDD[String] = rdd.map {
//      case (_, result) => {
//        Bytes.toString(result.getRow)
//      }
//    }
//    rdd2.collect().foreach(println)
//    sc.stop()
//
//  }
}
