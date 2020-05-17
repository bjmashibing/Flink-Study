package com.msb.stream.window

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer

//读取kafka数据，统计每隔10s最近30分钟内，每辆车的平均速度
object SlidingTimeWindowKeyedStream {
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
      (splits(1),splits(3).toLong)
    }).keyBy(_._1)
      .timeWindow(Time.minutes(30),Time.seconds(10))
      .aggregate(new AggregateFunction[(String,Long),(String,Long,Long),(String,Double)] {

        override def createAccumulator(): (String, Long, Long) = ("",0,0)

        override def add(value: (String, Long), accumulator: (String, Long, Long)): (String, Long, Long) = {
          (value._1,accumulator._2+value._2,accumulator._3 + 1)
        }

        override def getResult(accumulator: (String, Long, Long)): (String, Double) = {
          (accumulator._1,accumulator._2.toDouble/accumulator._3)
        }

        override def merge(a: (String, Long, Long), b: (String, Long, Long)): (String, Long, Long) = {
          (a._1,a._2+b._2,a._3+b._3)
        }
      }).print()
    env.execute()
  }
}
