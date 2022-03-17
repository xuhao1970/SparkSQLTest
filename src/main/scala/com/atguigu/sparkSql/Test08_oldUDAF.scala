package com.atguigu.sparkSql

import com.atguigu.sparkSql.Test07_UDAF.MyAvg
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * @author yhm
 * @create 2021-11-03 14:32
 */
object Test08_oldUDAF {
  def main(args: Array[String]): Unit = {

    // 1 创建上下文环境配置对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQLTest")

    // 2 创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    // 3 读取数据
    val df: DataFrame = spark.read.json("input/user.json")

    // 4 注册UDAF
    // 旧版的注册方法
    spark.udf.register("myAvg",new MyAvgUDAF())

    // 5 创建DataFrame临时视图
    df.createOrReplaceTempView("user")

    // 6 调用自定义UDAF函数
    spark.sql("select myAvg(age) from user").show()

    // 7 释放资源
    spark.stop()
  }

  class MyAvgUDAF extends UserDefinedAggregateFunction {

    // 聚合函数输入参数的数据类型：age(Long)
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("age",LongType)
      ))
    }

    // 聚合函数缓冲区中值的数据类型(age,count)
    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("sum",LongType),
        StructField("count",LongType)
      ))
    }

    // 函数返回值的数据类型
    override def dataType: DataType = DoubleType

    // 稳定性：对于相同的输入是否一直返回相同的输出。
    override def deterministic: Boolean = true

    // 函数缓冲区初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      // 存年龄的总和
      buffer.update(0, 0L)

      // 存年龄的个数
      buffer.update(1, 0L)
    }

    // 更新缓冲区中的数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getLong(1) + 1
      }
    }

    // 合并缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = {
      buffer.getLong(0).toDouble / buffer.getLong(1)
    }
  }
}

