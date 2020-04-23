package com.msb.stream.windowt

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


object TumblingCountWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(x => x._1)
      //当窗口内相同key的元素数大于等于5则触发窗口执行，窗口计算时 只是计算key对应元素超过5的这批数据，其他数据不计算
      .countWindow(5)
      .sum(1)
      .print()
    env.execute()
  }
}
