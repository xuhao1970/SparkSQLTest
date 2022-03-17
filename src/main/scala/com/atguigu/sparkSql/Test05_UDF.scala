package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 16:35
 */
object Test05_UDF {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val dataFrame: DataFrame = spark.read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    // 编写sql查询数据
    val frame: DataFrame = spark.sql(
      """
        |select
        | name,
        | age
        |from
        | user
        |where age > 18
        |""".stripMargin
    )
    frame.show()
    frame.createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        | *
        |from
        |t1
        |""".stripMargin).show()

    // 4.关闭sparkSession
    spark.close()
  }
}
