package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 16:15
 */
object Test04_DSAndDF {

  def main(args: Array[String]): Unit = {
    // 创建sparkSession的配置对象
    val conf = new SparkConf().setAppName("SparkSql").setMaster("local[*]")

    // 创建sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 使用sparkSession
//    val dataFrame: DataFrame = spark.read.json("input/user.json")
//
//    // 导入隐式转换
//    import spark.implicits._
//
//    val value: Dataset[User] = dataFrame.as[User]
//    value.show()
//
//    value.toDF().show()

    val frame: DataFrame = spark.read.csv("input/user.txt")
    //frame.show()
    import spark.implicits._

    val rdd1: RDD[Row] = frame.rdd
    val sc = spark.sparkContext
    val list: RDD[String] = sc.makeRDD(List("1", "2", "3"))
    val frame1: DataFrame = list.toDF("num")
    val rdd: RDD[Row] = frame1.rdd

    // 关闭spark
    spark.close()
  }

}
