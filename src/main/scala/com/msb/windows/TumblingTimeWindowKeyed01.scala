package com.msb.windows

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

//1、每隔10s钟 统计最近10s数据的wordcount
//2、单词为hello结果不输出 （写入数据库）
//3、获取当前窗口的起始时间、终止时间、窗口长度
object TumblingTimeWindowKeyed01 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)
    stream.flatMap(x=>{
      val rest = new ListBuffer[(String,Int)]()
      x.split(" ").foreach(x => {
        rest += ((x,1))
      })
      rest
    })
      .keyBy(x=>x._1)
      .timeWindow(Time.seconds(10))
//      .reduce(new ReduceFunction[(String, Int)] {
//        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
//          (value1._1,value2._2+value1._2)
//        }
//      },
//        //窗口计算完毕后，会调用window function函数，每组数据处理完毕后只会调用一次
//        //如果要将window计算结果写入到外部数据库，可以再windowfunction中实现入库的逻辑
//        new ProcessWindowFunction[(String,Int),(String,Int),String,TimeWindow] {
//          override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
//            val startTime = context.window.getStart
//            val endTime = context.window.getEnd
//            val length = endTime-startTime
//            println("窗口长度：" + length + "\t起始位置："+startTime + "\t结束位置：" + endTime)
//            out.collect(elements.head)
//          }
//        }
//      ).print()
        .aggregate(new AggregateFunction[(String,Int),(String,Int),(String,Int)] {
      override def createAccumulator(): (String, Int) = ("",0)

      override def add(value: (String, Int), accumulator: (String, Int)): (String, Int) = (value._1,value._2+accumulator._2)

      override def getResult(accumulator: (String, Int)): (String, Int) = accumulator

      override def merge(a: (String, Int), b: (String, Int)): (String, Int) = (a._1+b._1,a._2+b._2)
    }).print()

    env.execute()
  }
}
