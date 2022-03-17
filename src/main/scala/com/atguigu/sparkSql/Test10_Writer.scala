package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @author xuhao
 * @create 2021-11-03 18:02
 */
object Test10_Writer {
  def main(args: Array[String]): Unit = {
    // 1.创建sparkSession配置对象
    val conf = new SparkConf().setAppName("sparkSql").setMaster("local[*]")

    // 2.创建一个sparkSession
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 3.使用sparkSession
    val dataFrame = spark.read.json("input/user.json")
    // 默认的写出格式是snappy + parquet 有压缩有列式存储
    //dataFrame.write.save("output")

    // 4.2 format指定保存数据类型
    // df.write.format("…")[.option("…")].save("…")
    // format("…")：指定保存的数据类型，包括"csv"、"jdbc"、"json"、"orc"、"parquet"和"text"。
    // save ("…")：在"csv"、"orc"、"parquet"和"text"(单列DF)格式下需要传入保存数据的路径。
    // option("…")：在"jdbc"格式下需要传入JDBC相应参数，url、user、password和dbtable
    //dataFrame.write.format("json").save("output1")

    //写出的模式有4种
    //默认使用的是报错模式

    //追加模式
    //数据库的使用
    //dataFrame.write.mode(SaveMode.Append).format("json").save("output1")

    //覆盖模式
    //推荐在读取原始数据 进行sparkSQL分析计算完之后， 写入到新的文件夹时使用
    //dataFrame.write.mode(SaveMode.Overwrite).format("json").save("output1")

    //忽略模式
    //如果写出的文件夹已经存在 不执行操作
    dataFrame.write.format("json").save("output3")
    // 4.关闭sparkSession
    spark.close()
  }

}
