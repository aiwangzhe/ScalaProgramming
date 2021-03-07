package com.wangzhe.spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkReadkafka {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("readKafka")
      .setMaster("local[2]")
    val sc = SparkContext.getOrCreate(conf)
    val streamContext = new StreamingContext(sc, Seconds(10));

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

    stream.map(record => (record.key, record.value)).print()

    streamContext.start()
    streamContext.awaitTermination()
  }
}
