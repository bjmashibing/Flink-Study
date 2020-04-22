package com.msb.stream.source

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala._

//flink 不消费kafka中的key值，只消费value
object ReadKafkaNoKey {
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

    stream.print()
    env.execute()

  }
}
