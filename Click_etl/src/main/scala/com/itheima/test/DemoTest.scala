package com.itheima.test

import com.itheima.main.ETLApp
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object DemoTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val spark = SparkSession.builder()
      .master("local[*]")
      .config(conf).appName(ETLApp.getClass.getName).getOrCreate()
    //获取上下文
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("a", 1), ("e", 1), ("d", 1), ("b", 1)), 2)
    rdd.mapPartitionsWithIndex((index, iter) => {

      iter.foreach {
        x =>
          println(x + ":" + index)
      }
      iter
    }).count()

    val sortRdd: RDD[(String, Int)] = rdd.sortByKey()

    sortRdd.mapPartitionsWithIndex(
      (index, iter) => {


        iter.foreach {
          x =>
            println(x + ":" + index)
        }
        iter
      }
    ).count()

    //    sortRdd.foreachPartition(
    //      (iter)=>{
    //       iter.foreach(println(_))
    //      }
    //    )


    sc.stop()
  }
}
