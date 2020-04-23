package com.msb.stream.windowt

import java.lang

import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TumblingTimeWindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.socketTextStream("node01", 8888)

    stream
      .flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(x => x._1)
//      .timeWindow(Time.seconds(10))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      //处理窗口时间内 根据key分好组的每组数据
      //      .process(new ProcessWindowFunction[(String,Int),String,String,TimeWindow] {
      //      //key：当前组中key都是一致的  elements：当前分组中所有数据（String，Int）  out：发射器往下游发送数据
      //          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
      //            var  count = 0
      //            for (elem <- elements) {
      //              count += 1
      //            }
      //            out.collect(key + " count:" + count)
      //          }
      //        }).print()
      //      .reduce(new ReduceFunction[(String, Int)] {
      //      override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      //        (value1._1, value1._2 + value2._2)
      //      }
      //    }).print()
      //        .sum(1).print()
      //        .aggregate(AggregationType.SUM,1)
//      .aggregate(new AggregateFunction[(String, Int), (String, Int), (String, Int)] {
//      override def createAccumulator(): (String, Int) = ("", 0)
//
//      override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = {
//        (value._1, value._2 + accumulator._2)
//      }
//
//      override def getResult(accumulator: (String, Int)): (String, Int) = accumulator
//
//      override def merge(a: (String, Int), b: (String, Int)): (String, Int) = {
//        (a._1, a._2 + b._2)
//      }
//    })
//      .print()
//        .aggregate(new AggregateFunction[(String,Int),(String,Int),(String,Int)] {
//      override def createAccumulator(): (String, Int) = ("",0)
//
//      override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = (value._1, value._2 + accumulator._2)
//
//      override def getResult(accumulator: (String, Int)): (String, Int) = accumulator
//
//      override def merge(a: (String, Int), b: (String, Int)): (String, Int) = (a._1, a._2 + b._2)
//    },
//      /**
//        * Base interface for functions that are evaluated over keyed (grouped) windows.
//        *
//        * @tparam IN The type of the input value.   输入的数据类型（第一个函数的聚合结果类型）
//        * @tparam OUT The type of the output value. 输出的数据类型
//        * @tparam KEY The type of the key.          分组key的类型
//        */
//      new WindowFunction[(String, Int),(String, Int),String,TimeWindow] {
//
//        //input 聚合结果
//      override def apply(key: String, window: TimeWindow, input: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
//        println("input.size:" + input.size)
//        for (elem <- input) {
//          out.collect(elem)
//        }
//      }
//    }).print()
        .aggregate(new AggregateFunction[(String,Int),(String,Int),(String,Int)] {
            override def createAccumulator(): (String, Int) = ("",0)

            override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = (value._1, value._2 + accumulator._2)

            override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

            override def merge(a: (String, Int), b: (String, Int)): (String, Int) = (a._1, a._2 + b._2)
          },
      new ProcessWindowFunction[(String, Int),(String, Int),String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          println("input.size:" + elements.size)
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      }).print()
    env.execute()
  }
}
