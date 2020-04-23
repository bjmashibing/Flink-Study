package com.msb.stream.windowt

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingTimeWindowsTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(x => x._1)
      //window每隔5s钟触发执行一次，每次处理最近10s的数据
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .sum(1)
      .print()

    env.execute()
  }
}
