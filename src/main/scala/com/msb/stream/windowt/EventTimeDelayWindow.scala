package com.msb.stream.windowt

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object EventTimeDelayWindow {

  class MyTimestampAndWatermarks(delayTime:Long) extends AssignerWithPeriodicWatermarks[String] {

    var maxCurrentWatermark: Long = _

    //水印=最大事件时间-延迟时间
    override def getCurrentWatermark: Watermark = {
      //产生水印
      new Watermark(maxCurrentWatermark - delayTime)
    }

    //获取当前的时间戳
    override def extractTimestamp(element: String, previousElementTimestamp: Long): Long = {
      val currentTimeStamp = element.split(" ")(0).toLong
      maxCurrentWatermark = math.max(currentTimeStamp,maxCurrentWatermark)
      currentTimeStamp
    }
  }

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100)
    val stream = env.socketTextStream("node01", 8888)
      .assignTimestampsAndWatermarks(new MyTimestampAndWatermarks(3000L))

    stream
      .flatMap(x => x.split(" ").tail)
      .map((_, 1))
      .keyBy(_._1)
      //      .timeWindow(Time.seconds(10))
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String, Int), (String, Int), String, TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String, Int)], out: Collector[(String, Int)]): Unit = {
          val start = context.window.getStart
          val end = context.window.getEnd
          var count = 0
          for (elem <- elements) {
            count += elem._2
          }
          println("start:" + start + " end:" + end + " word:" + key + " count:" + count)
        }
      })
      .print()

    env.execute()
  }
}
