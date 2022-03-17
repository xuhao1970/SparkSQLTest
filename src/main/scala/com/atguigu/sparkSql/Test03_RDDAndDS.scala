package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 15:41
 */
object Test03_RDDAndDS {
  def main(args: Array[String]): Unit = {
    // 1. 创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2. 创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc = spark.sparkContext

    // 3.使用sparkSession
    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 20)))

    // 相互转换的时候 需要导入隐式参数
    import spark.implicits._

    val ds: Dataset[User] = userRdd.toDS()
    ds.show()

    // 将样例类的ds转换为Rdd
    val rdd: RDD[User] = ds.rdd
    rdd.foreach(println)

    // 如果是普通类型的rdd转换为ds
    val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5), 2)

    val ds1: Dataset[Int] = rdd2.toDS
    ds1.show()

    spark.close()
  }
}
