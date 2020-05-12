package com.msb.test

import java.sql.DriverManager

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//1、每隔10s钟 统计最近10s数据的wordcount
//2、将窗口数据写入到mysql数据库中
//3、获取当前窗口的起始时间、终止时间、窗口长度
object TumblingTimeWindowKeyed01 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("node01",8888)
    stream.flatMap(x=>{
      val rest = new ListBuffer[(String,Int)]()
      x.split(" ").foreach(x => {
        rest += ((x,1))
      })
      rest
    })
      .keyBy(x=>x._1)
      .timeWindow(Time.seconds(10),Time.seconds(3))
        .aggregate(new AggregateFunction[(String,Int),(String,Int),(String,Int)] {
          override def createAccumulator(): (String, Int) = ("",0)

          override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = (value._1,value._2+accumulator._2)

          override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

          override def merge(a: (String, Int), b: (String, Int)): (String, Int) = (a._1,a._2+b._2)
        }).print()

    env.execute()
  }
}
