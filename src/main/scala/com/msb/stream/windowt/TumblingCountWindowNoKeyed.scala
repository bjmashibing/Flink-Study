package com.msb.stream.windowt

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object TumblingCountWindowNoKeyed {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      //窗口内元素大于等于5则触发执行，并不是看某一个key了，这是一个全窗口函数
      .countWindowAll(5)
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
