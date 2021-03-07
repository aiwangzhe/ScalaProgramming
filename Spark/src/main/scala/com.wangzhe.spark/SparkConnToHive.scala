package com.wangzhe.spark

import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import scala.collection.mutable.MutableList
import scala.util.Random

class Store(val s_city: String, val s_county: Int) {
  def this() {
    this(null, 0)
  }
}

case class Ratings(userId: Int, movieId: Int, rating: Double, timestamp: Long)

case class Student(userId: Int, username: String, age: Int, sex: String, address: String,
                   phone: Int, favorite: String, gradeId: Int, classId: Int,
                   height: Double, weight: Double)

object SparkConnToHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Hive Example")
      //.config("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

//    implicit val encoder: Encoder[Ratings] = Encoders.bean(classOf[Ratings])
//    val dataList: MutableList[Ratings] = MutableList()
//    val random = new Random()
//    for (i <- 1 to 100000) {
//      dataList += Ratings(i, i+2, random.nextDouble(), System.currentTimeMillis())
//    }

    implicit val encoder: Encoder[Student] = Encoders.bean(classOf[Student])

    val dataList: MutableList[Student] = MutableList()
    val random = new Random()
    for(i <- 1 to 100000) {
      val name = RandomStringUtils.randomAlphanumeric(10)
      val sex = if (random.nextDouble() < 0.5) "F" else "M"
      val address = RandomStringUtils.randomAlphanumeric(80)
      val phone = random.nextInt(1345678932)
      val favorite = RandomStringUtils.randomAlphanumeric(40)
      val gradeId = random.nextInt(20)
      val classId = random.nextInt(10)
      val height = random.nextDouble() * 200
      val weight = random.nextDouble() * 100
      dataList += Student(i, name, random.nextInt(100),
        sex, address, phone, favorite, gradeId, classId, height, weight)
    }

    spark.createDataFrame(dataList).createOrReplaceTempView("ratings_temp")
    //spark.sql("select * from ratings_temp").show()
    spark.sql("insert into test2.student select * from ratings_temp")


//    val dataset: Dataset[Store] = df.as
//    println("---------------start count-----------------------")
//    dataset.rdd.count()
//    println("count: " + dataset.count())
//    dataset.rdd.map(store => {
//      val country = store.s_county
//      val city = store.s_city
//      println("country: " + country)
//      println("city: " + city)
//      ((country, city), 1)
//    }).reduceByKey(_ + _).foreach(tuple2 => {
//      println(tuple2._1 + "-----------" + tuple2._2)
//    })
  }
}