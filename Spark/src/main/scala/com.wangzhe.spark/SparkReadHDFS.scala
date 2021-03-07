package com.wangzhe.spark

import java.io.File

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

case class Ratings(userId: String, movieId: String, score: String, timeStamp: String) {

}

object SparkReadHDFS {
  def main(args: Array[String]): Unit = {
    val jars = new File("/opt/spark-2.3.2-bin-hadoop2.6/jars")
      .list().map(name => "/opt/spark-2.3.2-bin-hadoop2.6/jars/" + name).mkString(",")
    System.out.println(jars)
    val conf = new SparkConf().setMaster("local[2]").setAppName("readHdfs")
      .setSparkHome("/usr/lib/spark/jars")
      .set("spark.yarn.jars", jars)
      //.setJars(Seq("Spark/target/Spark-1.0.jar"))
    val sc = new SparkContext(conf)
    //sc.textFile("/sfds").count()
    sc.textFile("/data/ratings.csv").map(
      str => {
        val splits = str.split(",")
        (splits(0), Ratings(splits(0), splits(1), splits(2), splits(3)))
      })
      .groupByKey().
      foreach(tuple2 => println("userId: " + tuple2._1 + ", movieIds: " + tuple2._2))
  }
}
