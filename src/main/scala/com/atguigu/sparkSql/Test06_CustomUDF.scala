package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 16:43
 */
object Test06_CustomUDF {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val dataFrame: DataFrame = spark.read.json("input/user.json")

    // 创建视图
    dataFrame.createOrReplaceTempView("user")

    spark.udf.register("changeName",(str:String) => s"${str}gongzi")

    val frame = spark.sql(
      """
        |select
        | changeName(name),
        | age
        |from
        | user
        |""".stripMargin)
    frame.show()

    // 4.关闭sparkSession
    spark.close()
  }

}
