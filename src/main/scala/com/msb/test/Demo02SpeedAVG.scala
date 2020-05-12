package com.msb.test

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
//每隔10s统计每辆汽车的平均速度
object Demo02SpeedAVG {
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

    stream.map(data => {
      val splits = data.split("\t")
      (splits(1),splits(3).toInt)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .aggregate(new AggregateFunction[(String,Int),(String,Int,Int),(String,Double)] {
        override def createAccumulator(): (String, Int, Int) = ("",0,0)

        override def add(value: (String, Int), accumulator: (String, Int, Int)): (String, Int, Int) = {
          (value._1,value._2+accumulator._2,accumulator._3+1)
        }

        override def getResult(accumulator: (String, Int, Int)): (String, Double) = {
          (accumulator._1,accumulator._2.toDouble/accumulator._3)
        }

        override def merge(a: (String, Int, Int), b: (String, Int, Int)): (String, Int, Int) = {
          (a._1,a._2+b._2,a._3+b._3)
        }
      }).print()

    env.execute()
  }
}
