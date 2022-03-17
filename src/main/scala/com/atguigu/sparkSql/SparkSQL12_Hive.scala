package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-04 10:54
 */
object SparkSQL12_Hive {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","atguigu")

    // 创建上下文配置对象
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")

    // 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    // 连接外部Hive,并进行操作
    ///spark.sql("show tables").show()
    //spark.sql("create table user7(id int,name string)").show()
    spark.sql("insert into user7 values(3,'pp')").show()
    spark.sql("select id,name from user7").show()

    // 释放资源
    spark.stop()
  }
}
