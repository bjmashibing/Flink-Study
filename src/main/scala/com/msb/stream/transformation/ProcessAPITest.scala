package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 定时器 应用场景：数据延迟   5s
  *
  * 对账系统
  *
  * app
  * 真正
  * app成功  银行对账
  */
object ProcessAPITest {

  case class CarInfo(carId: String, speed: Long)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream.map(data => {
      val splits = data.split(" ")
      val carId = splits(0)
      val speed = splits(1).toLong
      CarInfo(carId, speed)
    }).keyBy(_.carId)
      //KeyedStream调用process需要传入KeyedProcessFunction
      //DataStream调用process需要传入ProcessFunction
      .process(new KeyedProcessFunction[String, CarInfo, String] {

      override def processElement(value: CarInfo, ctx: KeyedProcessFunction[String, CarInfo, String]#Context, out: Collector[String]): Unit = {
        val currentTime = ctx.timerService().currentProcessingTime()
        if (value.speed > 100) {
          val timerTime = currentTime + 2 * 1000
          ctx.timerService().registerProcessingTimeTimer(timerTime)
        }
      }

      override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, CarInfo, String]#OnTimerContext, out: Collector[String]): Unit = {
        var warnMsg = "warn... time:" + timestamp + "  carID:" + ctx.getCurrentKey
        out.collect(warnMsg)
      }
    }).print()

    env.execute()
  }
}
