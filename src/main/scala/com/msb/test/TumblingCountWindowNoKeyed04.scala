package com.msb.test

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

object TumblingCountWindowNoKeyed04 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream
      .map(x=>{
        val splits = x.split(" ")
        (splits(0),splits(1).toLong)
      })
      .countWindowAll(5)
      .reduce((v1:(String,Long),v2:(String,Long))=>{
        (v1._1 + "|" + v2._1,v1._2+v2._2)
      })
      .print()
    env.execute()
  }
}
