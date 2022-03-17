package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 18:26
 */
object Test12_MysqlWrite {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val sc = spark.sparkContext
    val rdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 10), User("lisi", 4)))

    import spark.implicits._
    val dataFrame = rdd.toDF("name", "age")

    // 写出ds到mysql
    // 如果写入的表格设定有主键 需要注意主键不能重复 否则会报错
    dataFrame.write.format("jdbc")
        .option("url","jdbc:mysql://xuhao001:3306/gmall")
        .option("driver","com.mysql.jdbc.Driver")
        .option("user","root")
        .option("password","123456")
        .option("dbtable","user_info1")
        //数据库的操作不能使用覆盖模式
        .mode(SaveMode.Append)
        .save()

    // 4.关闭sparkSession
    spark.close()
  }

}
