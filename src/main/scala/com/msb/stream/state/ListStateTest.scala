package com.msb.stream.state

import java.text.SimpleDateFormat
import java.util.Properties

import com.msb.stream.state.ValueStateTest.CarInfo
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.common.serialization.StringSerializer
import scala.collection.JavaConverters._

/**
  * 统计出每一辆车的运行轨迹
  *
  * 1、拿到每辆车的所有信息（车牌号、卡口号、eventtime、speed）
  * 2、根据每辆车 分组
  * 3、对每组数据钟的信息按照eventtime排序 升序   卡口连接起来  轨迹
  *
  */
object ListStateTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","flink-kafka-001")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)

    //卡口号、车牌号、事件时间、车速
    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka",new SimpleStringSchema(),props))

    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:sss")

    stream.map(data => {
      val arr = data.split("\t")
      val time = format.parse(arr(2)).getTime
      (arr(0),arr(1),time,arr(3).toLong)
    }).keyBy(_._2)
      .map(new RichMapFunction[(String,String,Long,Long),(String,String)] {

        private var carInfos:ListState[(String,Long)] = _

        override def open(parameters: Configuration): Unit = {
          val desc = new ListStateDescriptor[(String,Long)]("list",createTypeInformation[(String,Long)])
          carInfos = getRuntimeContext.getListState(desc)
        }

        override def map(elem: (String, String, Long, Long)): (String, String) = {
          carInfos.add((elem._1,elem._3))

          val seq = carInfos.get().asScala.seq
          val sortList = seq.toList.sortBy(_._2)

          val builder = new StringBuilder
          for (elem <- sortList) {
            builder.append(elem._1 + "\t")
          }
          (elem._2,builder.toString())
        }
      }).print()

    env.execute()
  }
}
