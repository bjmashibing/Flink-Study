package com.msb.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object SlidingCountWindowTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      //当某个key对应的元素超过5则触发一次，计算当前key最近10条数据
      .countWindowAll(5,2)
      .max(0)
      .print()

    env.execute()

  }
}
