package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 17:55
 */
object Test09_Read {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession的配置对象
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")
    // 2. 创建sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 3. 使用sparkSession
    val reader: DataFrameReader = spark.read

    // 直接读取指定类型的问题
    reader.csv("input/user.txt").show()
    reader.json("input/user.json").show()

    // 标准化读取数据
    // 3.2 format指定加载数据类型
    // spark.read.format("…")[.option("…")].load("…")
    // format("…")：指定加载的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"
    // load("…")：在"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"格式下需要传入加载数据路径
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
    val frame = reader.format("json").load("input/user.json")
    val dataFrame = reader.format("csv").load("input/user.txt")

    frame.show()
    dataFrame.show()

    // 4.关闭
    spark.close()
  }
}
