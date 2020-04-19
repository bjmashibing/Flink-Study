package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

//split算子 可以根据某一些条件来拆分数据流
object SplitOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //偶数分到一个流(first)  奇数分到另外一个流(second)
    val stream = env.generateSequence(1,100)
    val splitStream: SplitStream[Long] = stream.split(d => {
      d % 2 match {
        case 0 => List("first")
        case 1 => List("second")
      }
    })
    splitStream.select("first").print().setParallelism(1)
    //select 算子可以通过标签 获取指定流

    env.execute()
  }
}
