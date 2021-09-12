package com.wangzhe.flink.flink.custormSource

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoWithMyParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //隐式转换
    import org.apache.flink.api.scala._

    val text = env.addSource(new MyParallelSourceScala).setParallelism(1)
      .map(line => {
        println("接受到的数据：" + line)
        line
      })

//    val mapData = text.map(line=>{
//      //println("接收到的数据："+line)
//      line
//    })
    val mapData = text
  .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ClickEvent](Time.seconds(1)) {
    override def extractTimestamp(element: ClickEvent): Long = element.timestamp
    })

  .timeWindowAll(Time.seconds(5))
  .aggregate(new MyCountAgg())
    mapData.print().setParallelism(1)


    //val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)


   // sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")



  }

  class MyCountAgg extends AggregateFunction[ClickEvent, Int, Int]{
    override def createAccumulator(): Int = 0

    override def add(value: ClickEvent, accumulator: Int): Int = accumulator + 1

    override def getResult(accumulator: Int): Int = accumulator

    override def merge(a: Int, b: Int): Int = a + b
  }
}
