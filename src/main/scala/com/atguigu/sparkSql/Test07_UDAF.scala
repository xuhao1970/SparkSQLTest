package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @author xuhao
 * @create 2021-11-03 16:48
 */
object Test07_UDAF {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val dataFrame = spark.read.json("input/user.json")

    dataFrame.createOrReplaceTempView("user")

    spark.udf.register("MyAvg",functions.udaf(new MyAvg))
    spark.sql(
      """
        |select
        | MyAvg(age)
        |from
        | user
        |""".stripMargin).show()

    // 4.关闭sparkSession
    spark.close()
  }

  case class Buff(var sum:Long,var count:Long)

  class MyAvg extends Aggregator[Long,Buff,Double]{
    override def zero: Buff = Buff(0,0)

    override def reduce(b: Buff, a: Long): Buff = {
      b.sum += a
      b.count +=1
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1
    }
    override def finish(reduction: Buff): Double = {
      reduction.sum.toDouble/reduction.count
    }

    // SparkSQL对传递的对象的序列化操作(编码)
    // 自定义的类型就是product 自带类型根据类型选择
    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}




