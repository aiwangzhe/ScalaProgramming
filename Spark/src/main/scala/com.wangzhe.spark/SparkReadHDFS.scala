package com.wangzhe.spark

import java.io.File

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}


object SparkReadHDFS {
  def main(args: Array[String]): Unit = {
    val jars = new File("/opt/spark-2.3.2-bin-hadoop2.6/jars")
      .list().map(name => "/opt/spark-2.3.2-bin-hadoop2.6/jars/" + name).mkString(",")
    System.out.println(jars)
    val conf = new SparkConf().setMaster("yarn").setAppName("readHdfs")
      .setSparkHome("/usr/lib/spark/jars")
      .set("spark.yarn.jars", jars)
      //.setJars(Seq("Spark/target/Spark-1.0.jar"))
    val sc = new SparkContext(conf)
    sc.hadoopFile[LongWritable, Text, TextInputFormat]("/test").map(s => (s, 1))
  }
}
