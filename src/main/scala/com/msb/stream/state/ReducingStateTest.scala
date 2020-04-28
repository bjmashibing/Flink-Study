package com.msb.stream.state

import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * 统计每辆车的速度综合  车牌号  speed
  * reduceingState
  */
object ReducingStateTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)

    stream.map(data => {
      val arr = data.split(" ")
      (arr(0),arr(1).toLong)
    }).keyBy(_._1)
      .map(new RichMapFunction[(String,Long),(String,Long)] {
          //定义ReducingState状态
        private var speedCount:ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ReducingStateDescriptor[Long]("reduce", new ReduceFunction[Long] {
            override def reduce(value1: Long, value2: Long): Long = value1 + value2
          }, createTypeInformation[Long])
          speedCount = getRuntimeContext.getReducingState(desc)
        }

        override def map(value: (String, Long)): (String, Long) = {
          speedCount.add(value._2)
          (value._1,speedCount.get())
        }
      }).print()
    env.execute()
  }
}
