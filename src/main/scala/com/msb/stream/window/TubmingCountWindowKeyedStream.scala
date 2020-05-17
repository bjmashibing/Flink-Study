package com.msb.stream.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * 当窗口中有10个元素，统计这10个元素的wordcount
  */
object TubmingCountWindowKeyedStream {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)

    stream
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      //如果基于keyed stream之上做count window 看的相同的元素是否大于10
      //窗口中没新增两个元素，窗口就会滑动一次（1、触发窗口计算 2、开启新窗口）
      .countWindow(10,2)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value1._2+value2._2)
        }
      }).print()

    env.execute()
  }
}
