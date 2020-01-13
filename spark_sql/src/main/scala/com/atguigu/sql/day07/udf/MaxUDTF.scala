package com.atguigu.sql.day07.udf

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}

object MaxUDTF {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("max").master("local[*]").getOrCreate()
    ss.udf.register("myMax",new MyMax)
    ss.udf.register("toDouble",{v:Long => v.toDouble})
    val df: DataFrame = ss.read.json("X:\\spark\\employees.json")
    df.createOrReplaceTempView("employees")
    ss.sql("select myMax(toDouble(salary)) from employees").show()
  }
}
class MyMax extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(StructField("inputColumn",LongType)::Nil)

  override def bufferSchema: StructType = StructType(StructField("max",LongType)::Nil)

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Long.MinValue
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
    if(!input.isNullAt(0)){
      buffer(0)=max(buffer.getLong(0),input.getLong(0))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=max(buffer1.getLong(0),buffer2.getLong(0))
  }

  override def evaluate(buffer: Row): Long = {
    buffer.getLong(0)
  }

  def max(a:Long,b:Long)={
    if(a>b) a else b
  }
}
