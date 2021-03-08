package com.wangzhe.spark

import com.google.gson.{Gson, GsonBuilder, JsonObject, JsonParser}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

case class UserData(dt: Long, usertype: String, username: String, area: String)

object SparkReadkafka {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readKafka")
      .setMaster("local[4]").set("spark.sql.warehouse.dir", "/user/hive/warehouse")
    //val sc = SparkContext.getOrCreate(conf)

    val session = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    session.sql("show databases").show()
    val streamContext = new StreamingContext(session.sparkContext, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "m5.leap.com:6667,m6.leap.com:6667,m7.leap.com:6667",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("allData")
    val stream = KafkaUtils.createDirectStream(streamContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))

    import session.implicits._

    stream.map(record => {
      new Gson().fromJson(record.value(), classOf[UserData])
    }).foreachRDD(rdd => {
      session.createDataFrame(rdd)
        .createTempView("userDataTemp")
      session.sql("insert into test2.userData select * from userDataTemp")
    })

    streamContext.start()
    streamContext.awaitTermination()
  }
}
