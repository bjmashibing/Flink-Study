package com.msb.stream.source

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

//flink 读取本地集合
object ReadCollections {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //一般不用
    val stream:DataStream[Int] = env.fromElements(1,2,3,4,5)

    stream.print()
    env.execute()

  }
}
