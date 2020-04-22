package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PartitonerTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.generateSequence(1,100).setParallelism(3)

    println(stream.getParallelism)
    stream.rebalance.print()
    env.execute()
  }
}
