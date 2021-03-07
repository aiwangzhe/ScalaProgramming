package com.wangzhe.spark

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object TestRddJoin {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("testJoin")
    val sc = SparkContext.getOrCreate(sparkConf)
    var data1 = mutable.MutableList[(Int, String)]()
    for (i <- 1 to 100) {
      data1 += Tuple2(i, "str" + i)
    }
    val broadcast = sc.broadcast(Array(1))
    var rdd1 = sc.parallelize(data1, 5)
    rdd1 = rdd1.map(x => {
      broadcast.value
      x
    })

    var data2 = mutable.MutableList[(Int, String)]()
    for (i <- 20 to 200) {
      data2 += Tuple2(i, "data2" + i)
    }
    val rdd2 = sc.parallelize(data2, 6)

    rdd1.join(rdd2).foreach(obj => println(obj))
  }
}
