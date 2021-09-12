package com.wangzhe.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.MutableList
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType}

case class YouTube (val videoId: String, val uploader: String, age: Int) {
  def this() {
    this(null, null, 0)
  }

  def getVideoId = {
    videoId
  }
}

object YouTube1 {
  def applyY(string: String): YouTube = {
    val strArr = string.split("\t")
    val videoId = strArr(0)
    val uploader = strArr(1)
    val age = strArr(2).trim.toInt
    var category = strArr(3).split("&")
    category = category.map(s => s.trim)
    val length = strArr(4).toInt
    val views = strArr(5).toInt
    val rate = strArr(6).toFloat
    val ratings = strArr(7).toInt
    val comments = strArr(8).toInt
    var relatedIdList = new mutable.MutableList[String]
    for (i <- 9 until strArr.length) {
      relatedIdList += strArr(i)
    }
    new YouTube(videoId, uploader, age)
  }
}

object SparkPutYouTubeDataToHive {
  //create table youtube2(videoId string, uploader string, age int, category array<string>, length int, views int, rate float, ratings int, comments int,relatedId array<string>)
  //row format delimited fields terminated by "\t"
  //collection items terminated by "&"
  //stored as parquet;


  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder().appName("SparkPutYouTubeDataToHive")
      .master("local[1]").enableHiveSupport().getOrCreate()
    implicit val encoder: Encoder[YouTube] = Encoders.bean(classOf[YouTube])

    sc.sql("use mytest")

    val productSchema = StructType(List(StructField("videoId",StringType,true),
      StructField("uploader",StringType,true),
      StructField("age",IntegerType,true)))

    val dataRdd = sc.read.textFile("file:///home/wangzhe/Documents/0222")
      .filter(line => line.split("\t").length > 2)
      .map[YouTube](line => {
        YouTube1.applyY(line)
      }).rdd

    sc.createDataFrame(dataRdd, classOf[YouTube]).printSchema()




//
//    sc.sqlContext.createDataFrame(dataRdd, classOf[YouTube])
//      .foreach(println(_))
      //.createTempView("youtubeview")
    //sc.sql("select * from youtubeview").show(10)
  }
}
