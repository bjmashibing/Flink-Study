package com.msb.test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

/**
  * 每隔10s对窗口内所有汽车的车速进行排序
  */
object Demo03SortSpeed {
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
    }).timeWindowAll(Time.seconds(10))
      //注意：想要全局排序并行度需要设置为1
      .process(new ProcessAllWindowFunction[(String,Int),String,TimeWindow] {
        override def process(context: Context, elements: Iterable[(String, Int)], out: Collector[String]): Unit = {
          val sortList = elements.toList.sortBy(_._2)
          for (elem <- sortList) {
            out.collect(elem._1+" speed:" + elem._2)
          }
        }
      }).setParallelism(1).print().setParallelism(1)
    env.execute()
  }
}
