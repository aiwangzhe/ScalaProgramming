package com.wangzhe.flink.flink.custormSource

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import java.text.SimpleDateFormat
import scala.util.Random

/**
  * 创建自定义并行度为1的source
  *
  * 实现从1开始产生递增数字
  *
  * Created by xuwei.tech on 2018/10/23.
  */

case class ClickEvent(userId: Int, timestamp: Long) {
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  override def toString: String = {
    val timeStr = sdf.format(timestamp)
    s"ClickEvent($userId, $timeStr)"
  }
}

class MyParallelSourceScala extends ParallelSourceFunction[ClickEvent]{

  var count = 1L
  var isRunning = true

  override def run(ctx: SourceContext[ClickEvent]) = {
    while(isRunning){
      val userId = Random.nextInt(10)
      val timestamp = System.currentTimeMillis() - Random.nextInt(5) * 1000
      ctx.collect(ClickEvent(userId, timestamp))
      count+=1
      Thread.sleep(1000)
    }

  }

  override def cancel() = {
    isRunning = false
  }
}
