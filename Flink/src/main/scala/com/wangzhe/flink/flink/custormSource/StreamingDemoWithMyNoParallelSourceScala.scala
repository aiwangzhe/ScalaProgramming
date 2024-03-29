package com.wangzhe.flink.flink.custormSource

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import xuwei.tech.streaming.custormSource.MyNoParallelSourceScala

/**
  * Created by xuwei.tech on 2018/10/23.
  */
object StreamingDemoWithMyNoParallelSourceScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala.createTypeInformation

    val text = env.addSource(new MyNoParallelSourceScala)

    val mapData = text.map(line=>{
      println("接收到的数据："+line)
      line
    })

    val sum = mapData.timeWindowAll(Time.seconds(2)).sum(0)


    sum.print().setParallelism(1)

    env.execute("StreamingDemoWithMyNoParallelSourceScala")



  }

}
