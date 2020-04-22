package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Rescale {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(1,10).setParallelism(2)

    stream.writeAsText("./data/stream1").setParallelism(2)

    env.execute()

    /**
      * 1、大数据运维
      * 2、数据预处理
      * 3、数据采集
      * 4、MySQL
      */

  }
}
