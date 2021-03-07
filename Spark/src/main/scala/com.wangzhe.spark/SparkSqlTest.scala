package com.wangzhe.spark

import java.io.File

import org.apache.spark.sql.SparkSession

object SparkSqlTest {

  def main(args: Array[String]): Unit = {
    val jars = new File("/opt/spark-2.3.2-bin-hadoop2.6/jars")
      .list().map(name => "/opt/spark-2.3.2-bin-hadoop2.6/jars/" + name).mkString(",")

    val session = SparkSession.builder()
      .appName("Spark Hive Example")
      .master("yarn")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("spark.yarn.jars", jars)
      .config("spark.yarn.am.memory", "1024m")
      .enableHiveSupport()
      .getOrCreate()

    session.sql("use test")
    session.sql("select * from user1")

  }
}
