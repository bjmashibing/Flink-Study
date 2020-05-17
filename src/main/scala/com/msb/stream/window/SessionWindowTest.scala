package com.msb.stream.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, ProcessingTimeSessionWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object SessionWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)
    //session生命周期 10s   也就是说窗口如果连着10s中没有新的数据进入窗口，窗口就会滑动（触发计算）
//    EventTime   事件时间    Process Time：元素被处理的系统时间
//    stream.windowAll(EventTimeSessionWindows.withGap()).print()
    env.execute()
  }
}
