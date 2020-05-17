package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1、指定时间语义
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //2、在往socket发射数据的时候 必须携带时间戳
    val stream = env.socketTextStream("node01", 8888)
      //3、assignAscendingTimestamps  指定哪一个值代表的是Event Time
      .assignAscendingTimestamps(data => {
      val splits = data.split(" ")
      splits(0).toLong
    })

    stream
      .flatMap(x => x.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      //滚动窗口
      .timeWindow(Time.seconds(3))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value1._2 + value2._2)
        }
      },new WindowFunction[(String,Int),(String,Int),String,TimeWindow] {
        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          val start = window.getStart
          val end = window.getEnd
          println("window start:" + start + "--- end:" + end)
          for (elem <- input) {
            out.collect(elem)
          }
        }
      })
      .print()

    env.execute()
  }
}
