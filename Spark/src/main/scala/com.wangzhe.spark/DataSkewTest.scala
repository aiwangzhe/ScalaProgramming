package com.wangzhe.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.concurrent.duration.Duration

object DataSkewTest {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("testJoin")
    val sc = SparkContext.getOrCreate(sparkConf)
    var list = List(("hello", 1))
    val context = new StreamingContext(sc, Seconds(1000));
  }
}
