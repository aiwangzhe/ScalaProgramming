package com.wangzhe.spark

import java.io.{File, FilenameFilter}

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {
    val jars = new File("/opt/spark-2.3.2-bin-hadoop2.6/jars/")
        .list(new FilenameFilter {
          override def accept(dir: File, name: String): Boolean = {
            if(name.endsWith(".jar")) true else false
          }
        }).map(name => "/opt/spark-2.3.2-bin-hadoop2.6/jars/" + name).mkString(",")

    System.setProperty("spark.yarn.jars", jars)
    val sparkConf = new SparkConf()
    sparkConf.setJars(Seq("Spark/target/Spark-1.0.jar")).setMaster("yarn").setAppName("wordcount")
    val sc = SparkContext.getOrCreate(sparkConf)
    val array = Array(1, 8, 10, 34, 43, 89, 30, 10, 23, 123, 34, 10, 223)
    val rdd = sc.parallelize(array, 3)
    val groupedRdd = rdd.map(x => ("i" + x, x)).groupByKey(2)
    val resultRdd = groupedRdd.map(str => (str, 1)).reduceByKey((i1, i2) => i1 + i2).count()
    println("count: " + resultRdd)
    sc.stop()

  }
}
