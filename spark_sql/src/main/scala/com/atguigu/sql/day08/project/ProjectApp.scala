package com.atguigu.sql.day08.project

import org.apache.spark.sql.SparkSession

object ProjectApp {
  def main(args: Array[String]): Unit = {
    System.setProperty("SYSTEM_USER_NAME","atguigu")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().master("local[*]").appName("project").getOrCreate()
    spark.sql("use sparkpractice")
    spark.sql(
      """
        |select ci.*,
        |pi.product_name,
        |uv.click_product_id
        |from sparkpractice.user_visit_action uv
        |join sparkpractice.product_info pi on uv.click_product_id = pi.product_id
        |join sparkpractice.city_info ci on uv.city_id= ci.city_id
      """.stripMargin).createOrReplaceTempView("t1")

    //注册聚合函数
    spark.udf.register("remark",RemarkUDAF)
    spark.sql(
      """
        |select area,
        |product_name,
        |count(*) ct,
        |remark(city_name) remark
        |from t1
        |group by area,product_name
      """.stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select area,
        |product_name,
        |ct,
        |remark,
        |rank() over(partition by area order by ct desc) rk
        |from t2
      """.stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select area,
        |product_name,
        |ct,
        |remark
        |from t3
        |where rk <= 3
      """.stripMargin).show()

    spark.close()
  }
}
