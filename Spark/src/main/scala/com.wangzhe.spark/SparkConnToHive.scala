package com.wangzhe.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

class Store(val s_city: String, val s_county: Int) {
  def this() {
    this(null, 0)
  }
}

object SparkConnToHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    implicit val encoder: Encoder[Store] = Encoders.bean(classOf[Store])

    val df = spark.sql("select s_city, s_country from mytest.store")
    val dataset: Dataset[Store] = df.as
    println("---------------start count-----------------------")
    dataset.rdd.count()
    println("count: " + dataset.count())
    dataset.rdd.map(store => {
      val country = store.s_county
      val city = store.s_city
      println("country: " + country)
      println("city: " + city)
      ((country, city), 1)
    }).reduceByKey(_ + _).foreach(tuple2 => {
      println(tuple2._1 + "-----------" + tuple2._2)
    })
  }
}
