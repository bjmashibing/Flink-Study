package com.msb.test

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.mutable.ListBuffer

//每隔10s 统计最近10s的单词总数
object TumblingTimeWindowNoKeyed02 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)
    stream.flatMap(x=>{
      val rest = new ListBuffer[(String,Int)]()
      x.split(" ").foreach(x => {
        rest += ((x,1))
      })
      rest
    }).timeWindowAll(Time.seconds(10))
      .reduce((v1:(String,Int),v2:(String,Int))=>{
        (v1._1 + "," + v2._1,v1._2+v2._2)
      }).print()

    env.execute()
  }
}
