package com.msb.stream.windowt

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingTimeWindowNoKeyed {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .timeWindowAll(Time.seconds(10))
      //对于未经分组的数据累加
//      .sum(1)
      .reduce(new ReduceFunction[(String, Int)] {
      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
        (value1._1 + "|" + value2._1,value1._2+value2._2)
      }
    })
      .print()

    env.execute()
  }
}
