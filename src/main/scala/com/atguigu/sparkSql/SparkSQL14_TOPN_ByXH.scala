package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author xuhao
 * @create 2022-02-22 15:06
 */
object SparkSQL14_TOPN_ByXH {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")

    // TODO 1.创建上下文配置对象
    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    // TODO 2.创建SparkSession对象
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("use default")

    // TODO 3.注册自定义聚合函数
    spark.udf.register("city_remark",functions.udaf(new CityRemarkUDAF1))

    // TODO 4.Sql语句
    spark.sql(
      """
        |select
        |	area,
        |	product_name,
        |	number,
        |	city_info
        |from (
        |	select
        |		*,
        |		rank() over(partition by area order by number desc) rnk
        |	from (
        |		select
        |			area,
        |			product_name,
        |			count(*) number,
        |			city_remark(city_name) city_info
        |		from (
        |			select
        |				c.area,
        |				c.city_name,
        |				p.product_name
        |			from user_visit_action u
        |			join city_info c on u.city_id=c.city_id
        |			join product_info p on u.click_product_id=p.product_id
        |			where u.click_product_id > -1
        |		)t1
        |		group by area,product_name
        |	)t2
        |) t3
        |where rnk <= 3
        |""".stripMargin).show(1000,false)

    spark.stop()

  }

}

case class Buffer1(var totalCount: Long, var cityMap: mutable.Map[String,Long])

class CityRemarkUDAF1 extends Aggregator[String,Buffer1,String] {
  override def zero: Buffer1 = Buffer1(0L,mutable.Map[String,Long]())

  override def reduce(b: Buffer1, a: String): Buffer1 = {
    // 总个数加1
    b.totalCount += 1

    // 对应城市个数加1
    b.cityMap(a) = b.cityMap.getOrElse(a,0L) + 1

    b

  }

  override def merge(b1: Buffer1, b2: Buffer1): Buffer1 = {
    b1.totalCount += b2.totalCount
    b2.cityMap.foldLeft(b1.cityMap)((res,elem) => {
      res(elem._1) = res.getOrElse(elem._1,0L) + elem._2
      res
    })

    b1
  }

  override def finish(reduction: Buffer1): String = {
    val remarkList = ListBuffer[String]()

    val cityCountList: List[(String, Long)] = reduction.cityMap.toList.sortWith {
      case (left, right) => {
        left._2 > right._2
      }
    }.take(2)

    var sum: Double = 0L

    // 计算出前两名的百分比
    cityCountList.foreach{
      case (key,number) => {
        val r = number * 100 / reduction.totalCount
        remarkList.append(s"$key $r%")
        sum += r
      }
    }

    if (reduction.cityMap.size>2) {
      remarkList.append("其他 " + (100-sum).toInt + "%")
    }

    remarkList.mkString(",")
  }

  override def bufferEncoder: Encoder[Buffer1] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
