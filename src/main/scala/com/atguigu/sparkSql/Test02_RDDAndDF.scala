package com.atguigu.sparkSql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 13:54
 */
object Test02_RDDAndDF {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val rdd: RDD[String] = sc.textFile("input/user.txt")

    // 导入隐式转换
    import spark.implicits._

    // 将rdd转换为df
    // 默认情况下 df只有一列value
    val dataFrame: DataFrame = rdd.toDF()

    dataFrame.show()

    // 对rdd进行转换为二元组
    val tupleRdd = rdd.map(str => {
      val strings = str.split(",")
      (strings(0), strings(1).toLong)
    })

    // 指定列名
    val dataFrame1: DataFrame = tupleRdd.toDF("name","age")
    dataFrame1.show()

    // 将df转化为rdd
    val rdd1: RDD[Row] = dataFrame1.rdd

    rdd1.collect().foreach(elem => println(s"${elem.getString(0)}:${elem.getLong(1)}"))

    // 使用样例类相互转换
    // 如果rdd是样例类的数据类型 转换为df的时候会将样例类的属性读成为列
    val userRdd: RDD[User] = sc.makeRDD(List(User("zhangsan", 18), User("lisi", 20)))

    val frame: DataFrame = userRdd.toDF()
    frame.show()

    // 如果使用样例类的df转换为rdd 会丢失数据类型
    val rdd2: RDD[Row] = frame.rdd
    rdd2.collect().foreach(println)

    spark.close()

  }
}

case class User(name:String,age:BigInt)
