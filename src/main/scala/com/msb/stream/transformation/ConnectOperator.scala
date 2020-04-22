package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.co.{CoFlatMapFunction, CoMapFunction}
import org.apache.flink.streaming.api.scala.{ConnectedStreams, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * connect算子 也是将两个数据流进行合并
  * 优点：合并的这两个数据流中的元素类型可以不一样  union；元素类型必须一致
  */
object ConnectOperator {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val ds1 = env.socketTextStream("node01", 8888)
    val ds2 = env.socketTextStream("node01", 9999)
    val wcStream1 = ds1.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val wcStream2 = ds2.flatMap(_.split(" ")).map((_, 1)).keyBy(0).sum(1)
    val restStream: ConnectedStreams[(String, Int), (String, Int)] = wcStream2.connect(wcStream1)


//    restStream.map(new CoMapFunction[(String,Int),(String,Int),(String,Int)] {
//      //处理的是wcStream2中的元素
//      override def map1(value: (String, Int)): (String, Int) = {
//        println("wcStream2:" + value)
//        value
//      }
//
//      //处理的是wcStream1中的元素
//      override def map2(value: (String, Int)): (String, Int) = {
//        println("wcStream1:" + value)
//        value
//      }
//    })

    env.execute()
  }
}
