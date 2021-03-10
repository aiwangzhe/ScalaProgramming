package com.wangzhe.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkReadSocket {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readKafka")
      .setMaster("local[4]").set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    val sc = SparkContext.getOrCreate(conf)
    val streamContext = new StreamingContext(sc, Seconds(10))
    streamContext.socketStream()
  }
}
