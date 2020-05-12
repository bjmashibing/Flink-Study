package com.msb.test

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

object SlidingTimeWindowKeyed05 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.flatMap(x => {
      val rest = new ListBuffer[(String, Int)]()
      x.split(" ").foreach(x => {
        rest += ((x, 1))
      })
      rest
    })
      .keyBy(x => x._1)
      .timeWindow(Time.seconds(10), Time.seconds(5))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value2._2 + value1._2)
        }
      }).print()

    env.execute()
  }
}
