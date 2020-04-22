package com.msb.stream.transformation

import java.util.Properties

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTuple2TypeInformation, createTypeInformation}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringSerializer

/**
  * demo01：从kafka中消费数据，统计各个卡口的流量
  * demo02：从kafka中消费数据，统计每一个分钟每一个卡口的流量   构建组合key
  *       时间映射成分钟字段 ,然后wordcount
  */
object CarFlowAnaly {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //  key1 ： thread1 thread2
    env.setParallelism(80)

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
    //过滤掉key
    val valueStream = stream.map(_._2)

    //stream 中元素类型 变成二元组类型  kv stream   k:monitor_id v:1
    /**
      * 相同key的 数据一定是由某一个subtask处理
      * 一个subtask 可能会处理多个key所对应的数据
      */
    valueStream.map(data => {
      val splits = data.split("\t")
      val monitorID = splits(0)
      (monitorID,1)
    }).keyBy(x=>x._1)
      .reduce(new ReduceFunction[(String, Int)] {
        //v1：上次聚合的结果  v2：本次要聚合的数据
        override def reduce(v1: (String, Int), v2: (String, Int)): (String, Int) = {
          (v1._1,v1._2 + v2._2)
        }
      }).print()

    env.execute()

  }
}
