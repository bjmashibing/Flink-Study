package com.msb.stream.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AllowLateness {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env.socketTextStream("node01",8888)
    var lateTag =new OutputTag[(Long,String)]("late")
    val value = stream.map(x => {
      val strings = x.split(" ")
      (strings(0).toLong, strings(1))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[(Long, String)](Time.seconds(2)) {
      override def extractTimestamp(element: (Long, String)): Long = element._1
    }).timeWindowAll(Time.seconds(5))
      .allowedLateness(Time.seconds(3))
      .sideOutputLateData(lateTag)
      .process(new ProcessAllWindowFunction[(Long, String), (Long, String), TimeWindow] {
        override def process(context: Context, elements: Iterable[(Long, String)], out: Collector[(Long, String)]): Unit = {
          println(context.window.getStart + "---" + context.window.getEnd)
          for (elem <- elements) {
            out.collect(elem)
          }
        }
      })
    value.print("main")
    value.getSideOutput(lateTag).print("late")
    env.execute()
  }
}
