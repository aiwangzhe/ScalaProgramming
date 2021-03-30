package com.wangzhe.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkReadParquet {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("test")
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    session.read.parquet("/apps/hive/warehouse/itcast_ods.db/click_pageviews/dt=20191101")
      .take(5).foreach(row => print(row))
  }
}
