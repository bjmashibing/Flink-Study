package com.msb.stream.conntable

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

object BroadCastStream {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers","node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id","flink-kafka-001")
    props.setProperty("key.deserializer",classOf[StringSerializer].getName)
    props.setProperty("value.deserializer",classOf[StringSerializer].getName)
    val consumer = new FlinkKafkaConsumer[String]("configure",new SimpleStringSchema(),props)
    //从topic最开始的数据读取
//    consumer.setStartFromEarliest()
    //从最新的数据开始读取
    consumer.setStartFromLatest()

    //动态配置信息流
    val configureStream = env.addSource(consumer)
    //业务流
    val busStream = env.socketTextStream("node01",8888)

    //定义map state描述器
    val descriptor = new MapStateDescriptor[String,  String]("dynamicConfig",
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)

    //设置广播流的数据描述信息
    val broadcastStream = configureStream.broadcast(descriptor)

    //connect关联业务流与配置信息流，broadcastStream流中的数据会广播到下游的各个线程中
    busStream.connect(broadcastStream)
        .process(
          new BroadcastProcessFunction[String,String,String] {

            //每来一个新的元素都会调用一下这个方法
          override def processElement(line: String, ctx: BroadcastProcessFunction[String, String, String]#ReadOnlyContext, out: Collector[String]): Unit = {
            val broadcast = ctx.getBroadcastState(descriptor)
            val city = broadcast.get(line)
            if(city == null){
              out.collect("not found city")
            }else{
              out.collect(city)
            }
          }

          //kafka中配置流信息，写入到广播流中
          override def processBroadcastElement(line: String, ctx: BroadcastProcessFunction[String, String, String]#Context, out: Collector[String]): Unit = {
            val broadcast = ctx.getBroadcastState(descriptor)
            //kafka中的数据
            val elems = line.split(" ")
            broadcast.put(elems(0),elems(1))
          }
        }).print()
    env.execute()
  }
}
