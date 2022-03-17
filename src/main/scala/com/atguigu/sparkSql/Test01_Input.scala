package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 12:27
 */

object Test01_Input {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val dataFrame: DataFrame = spark.read.json("input/user.json")

    dataFrame.show()

    // 4.关闭sparkSession
    spark.close()
  }
}
