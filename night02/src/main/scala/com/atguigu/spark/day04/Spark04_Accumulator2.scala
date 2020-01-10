package com.atguigu.spark.day04

import org.apache.spark.util.AccumulatorV2

class Spark04_Accumulator2 extends AccumulatorV2[Long,Long]{

  private var sum :Long =0

  override def isZero: Boolean = {sum == 0}

  override def copy(): AccumulatorV2[Long, Long] = {
    val accumulator = new Spark04_Accumulator2
    accumulator.sum = this.sum
    accumulator
  }

  override def reset(): Unit = {sum = 0}

  override def add(v: Long): Unit = {sum = sum +  v}

  override def merge(other: AccumulatorV2[Long, Long]): Unit = other match{
    case s:Spark04_Accumulator2 => this.sum = s.sum + this.sum
    case _ => new IllegalStateException("非法状态异常")
  }

  override def value: Long = sum
}
