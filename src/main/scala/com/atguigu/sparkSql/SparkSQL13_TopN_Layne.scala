package com.atguigu.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkSQL13_TopN_Layne {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "atguigu")

    //TODO 1 创建SparkConf配置文件,并设置App名称
    val conf = new SparkConf().setAppName("SparkSQLTest").setMaster("local[*]")
    //TODO 2 利用SparkConf创建sparksession对象
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    //注册自定义UDAF函数
    spark.udf.register("city_remark", functions.udaf(new CityRemarkUDAF()))

    //执行sparksql
    spark.sql(
      """
        |select
        |    t3.area,
        |    t3.product_name,
        |    t3.click_count,
        |    t3.city_remark
        |from
        |(
        |    select
        |        t2.area,
        |        t2.product_name,
        |        t2.click_count,
        |        t2.city_remark,
        |        rank() over(partition by t2.area order by t2.click_count desc) rk
        |    from
        |    (
        |        select
        |            t1.area,
        |            t1.product_name,
        |            count(*) click_count,
        |            city_remark(t1.city_name) city_remark
        |        from
        |        (
        |            select
        |                c.area,
        |                c.city_name,
        |                p.product_name,
        |                v.click_product_id
        |            from user_visit_action v
        |            join city_info c
        |            on v.city_id = c.city_id
        |            join product_info p
        |            on v.click_product_id = p.product_id
        |            where v.click_product_id > -1
        |        )t1
        |        group by t1.area,t1.product_name
        |    ) t2
        |) t3
        |where t3.rk <= 3
        |""".stripMargin).show(1000, false)

    //TODO 3 关闭资源
    spark.stop()
  }

}

/**
 * 输入: city_name    String
 * 缓冲区: 这个大区的总点击量  Map[(City_name,城市的点击量)]   Buffer
 * 输出: city_remark   String
 */
case class Buffer(var totalcnt: Long, var cityMap: mutable.Map[String, Long])

class CityRemarkUDAF extends Aggregator[String, Buffer, String] {

  override def zero: Buffer = Buffer(0L, mutable.Map[String, Long]())

  //单个分区聚合方法  buffer 和   city
  override def reduce(buffer: Buffer, city: String): Buffer = {
    buffer.totalcnt += 1
    buffer.cityMap(city) = buffer.cityMap.getOrElse(city, 0L) + 1
    buffer
  }

  //多个buffer之间的聚合
  override def merge(b1: Buffer, b2: Buffer): Buffer = {
    b1.totalcnt += b2.totalcnt
    b2.cityMap.foreach {
      case (city, cityCnt) => {
        b1.cityMap(city) = b1.cityMap.getOrElse(city, 0L) + cityCnt
      }
    }
    b1
  }

  //最终逻辑计算方法
  override def finish(buffer: Buffer): String = {
    //0 定义一个listBuffer 用来存储最后返回结果
    val remarkList = ListBuffer[String]()

    //1 取出buffer的map,然后转成list,按照城市点击量 倒序排序
    val cityList: List[(String, Long)] = buffer.cityMap.toList.sortWith(
      (t1, t2) => t1._2 > t2._2
    )
    //2 取出排好序的cityList前两名,特殊处理
    var sum = 0L

    cityList.take(2).foreach {
      case (city, cityCnt) => {
        val res: Long = cityCnt * 100 / buffer.totalcnt
        remarkList.append(city + " " + res + "%")
        sum += res   //将前两个加起来,方便计算第三个其他百分比
      }
    }

    //3 计算第三个其他的百分比
    if (buffer.cityMap.size > 2) {
      remarkList.append("其他 " + (100 - sum) + "%")
    }

    //4 返回remarkList字符串
    remarkList.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}