package com.atguigu.spark.day04
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.shell.CopyCommands.Put
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapreduce.Job
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}

object Spark02_HbaseWrite {
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Spark01_Hbase").setMaster("local[2]")
//        val sc = new SparkContext(conf)
//        val configuration: Configuration = HBaseConfiguration.create()
//        configuration.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
//        configuration.set(TableOutputFormat.OUTPUT_TABLE, "student")
//        val job: Job = Job.getInstance(configuration)
//        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//        job.setOutputValueClass(classOf[Put])
//
//        val rdd: RDD[(String, String, String)] = sc.makeRDD(List(("1004","Jack","12"),("1005","Tom","13"),("1006","Maria","14"),("1007","Lion","15")))
//        val rdd2: RDD[(ImmutableBytesWritable, Put)] = rdd.map(x => {
//        val put = new Put(Bytes.toBytes(x._1))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(x._2))
//        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(x._3))
//        (new ImmutableBytesWritable(), put)
//
//    })
//    rdd2.saveAsNewAPIHadoopDataset(job.getConfiguration)
//    sc.stop()

  }
}
