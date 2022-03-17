package com.atguigu.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
 * @author xuhao
 * @create 2021-11-04 13:56
 */
object SparkSQL13_TopN {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","atguigu")

    val conf = new SparkConf().setAppName("SparkSQL").setMaster("local[*]")

    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()

    spark.sql("use default")

    //注册聚合函数
    spark.udf.register("city_remark",functions.udaf(new CityRemarkUDAF()))

    spark.sql(
      """
        |select
        | c.area,
        | c.city_name,
        | p.product_name,
        | u.click_product_id
        |from 
        | user_visit_action u
        |inner join city_info c
        |on u.city_id = c.city_id
        |inner join product_info p
        |on u.click_product_id=p.product_id
        |where u.click_product_id!=-1
        |""".stripMargin).createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        | area,
        | product_name,
        | count(*) click_count,
        | city_remark(city_name)
        |from t1
        |group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        | *,
        | rank() over(partition by area order by click_count desc) rnk
        |from t2
        |
        |""".stripMargin).createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        | *
        |from t3
        |where rnk<=3
        |""".stripMargin).createOrReplaceTempView("t4")

    spark.sql(
      """
        |select
        | *
        |from t4
        |""".stripMargin).show(1000,false)


    spark.close()

  }

  case class Buff(var totalcnt:Long,var cityMap:mutable.Map[String,Long])

  class CityRemarkUDAF extends Aggregator[String,Buff,String]{
    override def zero: Buff = Buff(0,mutable.Map())

    override def reduce(b: Buff, a: String): Buff = {
      b.totalcnt += 1L
      b.cityMap(a) = b.cityMap.getOrElse(a,0L) + 1L
      b
    }

    override def merge(b1: Buff, b2: Buff): Buff = {
      b1.totalcnt += b2.totalcnt
      for (elem <- b2.cityMap) {
        val key = elem._1
        val value = elem._2
        b1.cityMap(key) = b1.cityMap.getOrElse(key,0L) + value
      }
      b1
    }

    override def finish(reduction: Buff): String = {
      val remarkList = ListBuffer[String]()
      val total_count = reduction.totalcnt
      val list: List[(String, Long)] = reduction.cityMap.mapValues(num => num*100 / total_count).toList
      val list1 = list.sortWith(_._2 > _._2).take(2)
      var result:String = ""
      var sum:Double = 0
      for (elem <- list1) {
        val key = elem._1
        val value = elem._2
        remarkList.append(key + " " + value + "%")
        sum += value
      }
      if(reduction.cityMap.size>2){
        remarkList.append("其他 " + (100-sum) + "%")
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buff] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
