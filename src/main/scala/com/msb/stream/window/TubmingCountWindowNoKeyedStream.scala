package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * 当窗口中有10个元素，统计这10个元素的wordcount
  */
object TubmingCountWindowNoKeyedStream {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      //因为不是基于keyed stream之上的窗口，所以只需要看窗口中的元素数就可以，不需要看相同的元素的个数
      .countWindowAll(5)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1 + "," + value2._1, value1._2 + value2._2)
        }
      }).print()
    env.execute()
  }
}
