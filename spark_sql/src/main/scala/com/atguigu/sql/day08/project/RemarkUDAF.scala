package com.atguigu.sql.day08.project

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object RemarkUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("city_name",StringType)::Nil)

  override def bufferSchema: StructType =
    StructType(StructField("map",MapType(StringType,LongType))::StructField("total",LongType)::Nil)

  override def dataType: DataType = StructType(StructField("remark",StringType)::Nil)

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)= Map[String,Long]()
    buffer(1) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(1)=buffer.getLong(1)+1L
      val map: collection.Map[String, Long] = buffer.getMap[String,Long](0)
      buffer(0) = map + (input.getString(0) -> (map.getOrElse(input.getString(0),0L)+1L))
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1: collection.Map[String, Long] = buffer1.getMap[String,Long](0)
    val map2: collection.Map[String, Long] = buffer2.getMap[String,Long](0)
    buffer1(0)=map1.foldLeft(map2){
      case (map,(cityName,count))=>{
        map + (cityName ->(map.getOrElse(cityName,0L)+count))
      }
    }
    buffer1(1)=buffer1.getLong(1)+buffer2.getLong(1)
  }

  override def evaluate(buffer: Row): Any = {
    val total = buffer.getLong(1)
    val cityAndCount: collection.Map[String, Long] = buffer.getMap[String,Long](0)
    val cityAndCountTop2: List[(String, Long)] = cityAndCount.toList.sortBy(-_._2).take(2)
    val cityList: List[City] = cityAndCountTop2.map {
      case (cityName, count) => City(cityName, count.toDouble/total)
    }
    val result = cityList :+ City("其他" ,cityList.foldLeft(1D)(_-_.cityRatio))
    result.mkString(",")
  }
}
case class City(cityName:String,cityRatio:Double)