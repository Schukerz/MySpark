package com.atguigu.spark.day02.keyvalue

object spark01_either {
  def main(args: Array[String]): Unit = {

    val jack: Either[String, String] = testEither("jack")
    println(jack)
  }
  def testEither(name:String)={
    name match
    {
      case "jack" => Left("hello jack!")
      case _: String => Right("hello tom")
    }
  }
}
