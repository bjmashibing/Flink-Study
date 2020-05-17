package com.msb.stream.window

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer


/**
  * 需求：
  * 每隔一段时间对最近一段时间内，所有的车辆信息按照速度排序 并且获取最大最小速度的车辆信息
  *
  * 排序：全量聚合函数
  */
object Demo01 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","flink-kafka-001")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka",new SimpleStringSchema(),props))
    stream.map(data =>{
      val splits = data.split("\t")
      //车牌号   车速
      (splits(1),splits(3).toLong)
    }).timeWindowAll(Time.minutes(30),Time.seconds(5))
      .process(new ProcessAllWindowFunction[(String,Long),String,TimeWindow] {
        /**
          * @param context  上下文对象
          * @param elements  窗口中所有的元素
          * @param out      往下游发射数据的对象
          */
        override def process(context: Context, elements: Iterable[(String, Long)], out: Collector[String]): Unit = {
            //elements集合排序
          val sortList = elements.toList.sortBy(_._2)
          val maxSpeedCarInfo = sortList.last._1
          val minSpeedCarInfo = sortList.head._1
          println("最大车速的车牌号：" + maxSpeedCarInfo + "\t最小车速的车牌号：" + minSpeedCarInfo)
          for (elem <- sortList) {
            out.collect(elem._1)
          }
        }
      }).print()
    env.execute()
  }
}
