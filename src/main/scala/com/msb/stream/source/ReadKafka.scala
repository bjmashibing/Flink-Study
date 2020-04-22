package com.msb.stream.source

import java.util.Properties

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.flink.streaming.api.scala._

//读取kafka中数据  key value全部读出来
object ReadKafka {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","flink-kafka-001")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)

    //第一个参数 ： 消费的topic名
    val stream = env.addSource(new FlinkKafkaConsumer[(String, String)]("flink-kafka", new KafkaDeserializationSchema[(String, String)] {
      //什么时候停止，停止条件是什么
      override def isEndOfStream(t: (String, String)): Boolean = false

      //要进行序列化的字节流
      override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]): (String, String) = {
        val key = new String(consumerRecord.key(), "UTF-8")
        val value = new String(consumerRecord.value(), "UTF-8")
        (key, value)
      }

      //指定一下返回的数据类型  Flink提供的类型
      override def getProducedType: TypeInformation[(String, String)] = {
        createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[String])
      }
    }, props))
    stream.print()
    env.execute()
  }
}
