package com.atguigu.sparkSql

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 18:12
 */
object Test11_MysqlCon {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val reader: DataFrameReader = spark.read
    val pro = new Properties()
    pro.put("user","root")
    pro.put("password","123456")
    // 读mysql中的数据
    val dataFrame
      = reader.jdbc("jdbc:mysql://xuhao001:3306/gmall", "user_info", pro)
    //dataFrame.show()


    //使用标准的方法读取jdbc
    val frame = reader.format("jdbc")
      .option("url", "jdbc:mysql://xuhao001:3306/gmall")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user_info")
      .load()
    frame.show()

    // 4.关闭sparkSession
    spark.close()
  }

}
