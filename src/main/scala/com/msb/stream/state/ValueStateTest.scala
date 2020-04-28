package com.msb.stream.state

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object ValueStateTest {
  case class CarInfo(carId:String,speed:Long)
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //车牌号  speed
    val stream = env.socketTextStream("node01",8888)
    stream.map(data => {
      val splits = data.split(" ")
      CarInfo(splits(0),splits(1).toLong)
    }).keyBy(_.carId)
      .map(new RichMapFunction[CarInfo,String] {
        //private var speed:Long = _   但是  这样的变量不会C持久化到外部存储钟
        private var lastTempSpeed :ValueState[Long] =  _
        override def open(parameters: Configuration): Unit = {
          val desc = new ValueStateDescriptor[Long]("lastSpeed",createTypeInformation[Long])
          lastTempSpeed = getRuntimeContext.getState(desc)
        }

        override def map(value: CarInfo): String = {
          val lastSpeed = lastTempSpeed.value()
          this.lastTempSpeed.update(value.speed)
          if(lastSpeed != 0 && value.speed-lastSpeed > 30){
            "over speed " + value.toString
          }else{
            value.carId
          }
        }
      }).setParallelism(3).print()
    env.execute()
  }
}
