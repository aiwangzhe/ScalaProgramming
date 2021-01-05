package com.wangzhe.spark

import org.apache.spark.{SparkConf, SparkContext}


object SparkReadHDFS {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val sc = SparkContext.getOrCreate(sparkConf)

    sc.hadoopFile("/tmp").map(s => s + "sdfs")
  }
}
