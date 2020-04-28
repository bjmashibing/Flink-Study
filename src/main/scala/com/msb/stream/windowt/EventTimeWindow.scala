package com.msb.stream.windowt

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeWindow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.socketTextStream("node01", 8888).assignAscendingTimestamps(data => {
      val splits = data.split(" ")
      splits(0).toLong
    })

    stream
      .flatMap(x=>x.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
//      .timeWindow(Time.seconds(10))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      })
      .print()

    env.execute()
  }
}
