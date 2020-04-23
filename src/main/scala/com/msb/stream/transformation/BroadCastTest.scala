package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object BroadCastTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1,10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2)

    stream.broadcast.writeAsText("./data/stream2").setParallelism(4)
    env.execute()
  }
}
