package com.wangzhe.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkReadParquet {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("test")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    session.read.parquet("/user/hive/warehouse/itcast_ods.db/itcast_goods/dt=2019-09-09/00.parquet")
      .take(5).foreach(row => println(row))
  }
}
