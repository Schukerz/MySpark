package com.atguigu.sql.day07.udf

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator


object AvgDsUDTF {
  def main(args: Array[String]): Unit = {
    val ss: SparkSession = SparkSession.builder().appName("AVG").master("local[*]").getOrCreate()
    val df: DataFrame = ss.read.json("X:\\spark\\employees.json")
    df.createOrReplaceTempView("Employees")
    import ss.implicits._

    val ds: Dataset[Employee] = df.as[Employee]
    val selectEm: TypedColumn[Employee, Double] = new AVG().toColumn.name("my_avg")
    val resultDS: Dataset[Double] = ds.select(selectEm)
    resultDS.show()
  }
}
case class Employee(name:String,salary:Long)
case class SalaryAvg(salarySum:Long,salaryCount:Long){
  var salaryAvg:Long = salarySum/salaryCount
}
class AVG extends Aggregator[Employee,SalaryAvg,Double]{

  override def zero: SalaryAvg = SalaryAvg(0,0)

  override def reduce(b: SalaryAvg, a: Employee): SalaryAvg = a match{
    case e:Employee => SalaryAvg(b.salarySum+e.salary,b.salaryCount+1L)
    case null => b
  }

  override def merge(b1: SalaryAvg, b2: SalaryAvg): SalaryAvg = {
    SalaryAvg(b1.salarySum+b2.salarySum,b1.salaryCount+b2.salaryCount)
  }

  override def finish(reduction: SalaryAvg): Double = {
    reduction.salaryAvg
  }
  override def bufferEncoder: Encoder[SalaryAvg] = Encoders.product

  override def outputEncoder: Encoder[Double] = ???
}
