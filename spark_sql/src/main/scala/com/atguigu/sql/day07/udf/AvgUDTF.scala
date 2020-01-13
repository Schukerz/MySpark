package com.atguigu.sql.day07.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
object AvgUDTF{
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("AVG").master("local[*]").getOrCreate()
    val df: DataFrame = ss.read.json("X:\\spark\\employees.json")
    df.createOrReplaceTempView("Employees")

    ss.udf.register("my_avg",new MyAvg)
    ss.udf.register("myDouble",{s:Long=>s.toDouble})
    ss.sql("select my_avg(myDouble(salary)) from Employees").show()
  }
}
//class ToDouble extends User
class MyAvg extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(StructField("inputColumn",DoubleType)::Nil)

  override def bufferSchema: StructType =  StructType(StructField("sum",DoubleType)::StructField("count",LongType)::Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=0d
    buffer(1)=0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0)=buffer.getDouble(0)+input.getDouble(0)
      buffer(1)=buffer.getLong(1)+1L
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
//    if(!buffer2.isNullAt(0)){//可以initialze,所以不可能有null,if加不加都可以
      buffer1(0)=buffer1.getDouble(0)+buffer2.getDouble(0)
      buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
//    }
  }

  override def evaluate(buffer: Row): Any = {
    println(buffer(0)+","+buffer(1))
    buffer.getDouble(0)/buffer.getLong(1)
  }
}
