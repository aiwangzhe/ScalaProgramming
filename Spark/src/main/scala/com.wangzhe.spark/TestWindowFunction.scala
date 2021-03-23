package com.wangzhe.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
 
object TestWindowFunction {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("TestWindownFunction").getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
//  创建测试数据
    val ScoreDetailDF = sparkSession.createDataFrame(Seq(
      ("王五", "一年级", 98),
      ("李四", "一年级", 100),
      ("小民", "二年级", 90),
      ("小明", "二年级", 100),
      ("张三", "一年级", 100),
      ("小芳", "二年级", 95)
    )).toDF("name", "grade", "score")
 
    //    SparkSQL 方法实现
    ScoreDetailDF.createOrReplaceTempView("ScoreDetail")
    sparkSession.sql("SELECT * , " +
      "RANK() OVER (PARTITION BY grade ORDER BY score DESC) AS rank, " +
      "DENSE_RANK() OVER (PARTITION BY grade ORDER BY score DESC) AS dense_rank, " +
      "ROW_NUMBER() OVER (PARTITION BY grade ORDER BY score DESC) AS row_number " +
      "FROM ScoreDetail").show()
 
    //    DataFrame API 实现
    val windowSpec = Window.partitionBy("grade").orderBy(col("score").desc)
    ScoreDetailDF.select(col("name"),
      col("grade"),
      col("score"),
      rank().over(windowSpec).as("rank"),
      dense_rank().over(windowSpec).as("dense_rank"),
      row_number().over(windowSpec).as("row_number")
    ).show()
 
  }
 
}