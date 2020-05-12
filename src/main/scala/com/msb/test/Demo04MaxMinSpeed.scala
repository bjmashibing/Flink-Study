package com.msb.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object Demo04MaxMinSpeed {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))
    stream.map(data =>{
      val arr = data.split("\t")
      (arr(1),arr(3).toInt)
    }).timeWindowAll(Time.seconds(10))
      .process(new ProcessAllWindowFunction[(String,Int),String,TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          val sortList = elements.toList.sortBy(_._2)
          val minSpeedInfo = sortList.head
          val maxSpeedInfo = sortList.last
          val startWindowTime = context.window.getStart
          val endWindowTime = context.window.getEnd
          out.collect(
          "窗口起始时间："+startWindowTime  + "结束时间："+ endWindowTime +" 最小车辆速度车牌号：" + minSpeedInfo._1 + " 车速："+minSpeedInfo._2 + "\t最大车辆速度车牌号：" + maxSpeedInfo._1 + " 车速：" + maxSpeedInfo._2
          )
        }
        //The parallelism of non parallel operator must be 1
      }).print()
    env.execute()
  }
}
