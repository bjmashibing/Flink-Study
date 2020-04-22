package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartitonerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1,100)

    stream.print().setParallelism(1)
    env.execute()
  }
}
