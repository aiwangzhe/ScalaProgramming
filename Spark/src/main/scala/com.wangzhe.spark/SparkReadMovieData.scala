package com.wangzhe.spark

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object SparkReadMovieData {
  def main(args: Array[String]): Unit = {
    val session = SparkSession
      .builder()
      .appName("Read Movie Data")
      .config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .config("spark.driver.memory", "1024m")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    val ds = session.read.option("inferSchema", "true").option("header", "true").
      csv("file:///home/wangzhe/warehouse/movie.db/ratings/ratings.csv")

    ds.createOrReplaceTempView("ratings");
    val data = session.sql("select * from ratings").show()

    //data.write.parquet("/data/parquet_test_ratings/")

//    //session.sql("create database movie")
    //session.sql("use movie")
//    session.sql("drop table ratings")
//    session.sql("create external table ratings(userId int, movieId int, rating float, `timestamp` long)" +
//      "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' location '/data/movie/ratings'")
//    //session.sql("insert into ratings values(1, 'haha')");
    //session.sql("select userId, movieId, from_unixtime(timestamp, 'yyyy-MM-dd hh:mm:ss') from ratings").show()
  }
}
