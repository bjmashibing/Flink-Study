package com.msb.stream.sink

import java.util.{Date, Properties}

import com.msb.stream.util.DateUtils
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.util.Collector
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.serialization.StringSerializer

import scala.collection.mutable.ListBuffer

/**
  * 消费kafka中数据往hbase中写数据  redis
  * process（ProcessFunction   open close ）
  * 自定义sink  RichSinkFunction
  *
  *
  * 统计各个卡口的流量，将结果写入到HBase中
  */
object HBaseSinkTest {
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

    stream.map(data => {
        //monitorid carid eventtime speed
      val monitorId = data.split("\t")(0)
      (monitorId,1)
    }).keyBy(0)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
          (value1._1,value2._2+value1._2)
        }
      }).process(new ProcessFunction[(String,Int),(String,Int)] {
      var htab : HTable = _
      override def open(parameters: Configuration): Unit = {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum","node01:2181,node02:2181,node03:2181")
        val tableName = "car_flow"
        htab = new HTable(conf,tableName)
      }

      override def close(): Unit = {
        htab.close()
      }

      //每来一个元素 就调用一次
      override def processElement(value: (String, Int), ctx: ProcessFunction[(String, Int), (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        //key: monitorId   列名：分钟  value：count
        val min = DateUtils.getMin(new Date())
        val put = new Put(Bytes.toBytes(value._1))
        put.addColumn(Bytes.toBytes("count"),Bytes.toBytes(min),Bytes.toBytes(value._2))
        htab.put(put)
      }
    })
    env.execute()
  }
}
