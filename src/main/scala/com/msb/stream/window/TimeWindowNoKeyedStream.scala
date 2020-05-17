package com.msb.stream.window

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindowNoKeyedStream {
  def main(args: Array[String]): Unit = {

    //10s 统计 最近1年  全国总共车流量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)
    stream.timeWindowAll(Time.seconds(10))
      .reduce(_+_)




  }
}
