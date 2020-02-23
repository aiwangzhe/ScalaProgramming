package com.wangzhe.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkConnToHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      .master("yarn")
      .getOrCreate()

    val reader = spark.read.csv("hdfs://node2.leap.com:8082/apps/hive/warehouse/mydb.db/test")
    print(reader.rdd.count())
  }
}
