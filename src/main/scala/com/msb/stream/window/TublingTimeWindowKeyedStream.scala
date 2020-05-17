package com.msb.stream.window

import java.sql.DriverManager

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 1、每隔10s 计算最近Ns数据的wordcount
  * 2、将每个窗口的计算结果写入到mysql中
  */
object TublingTimeWindowKeyedStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val initStream: DataStream[String] = env.socketTextStream("node01", 8888)
    val wordStream = initStream.flatMap(_.split(" "))
    val pairStream = wordStream.map((_, 1))
    //是一个已经分好流的无界流
    val keyByStream = pairStream.keyBy(_._1)
    keyByStream
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1, value2._2 + value1._2)
        }
      },new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
        }
      }).print()
    env.execute()
  }
}
