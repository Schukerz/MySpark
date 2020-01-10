package com.atguigu.spark.day03_test

object Spark04_ContextGeneric {
  def main(args: Array[String]): Unit = {

    implicit val ord: Ordering[User] = new Ordering[User] {
      override def compare(x: User, y: User) = x.age - y.age
    }
    println(max(User(10, "a"), User(20, "n")))

  }
  def max[T:Ordering](a:T,b:T)={
    val ord: Ordering[T] = implicitly[Ordering[T]]
    if(ord.gteq(a,b)) a else b
  }
}
case class User(age:Int,name:String){

}
