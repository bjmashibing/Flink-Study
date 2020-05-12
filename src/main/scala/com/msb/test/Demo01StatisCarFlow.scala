package com.msb.test

import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector
import org.apache.kafka.common.serialization.StringSerializer

/**
  * 使用增量聚合函数统计最近20s内，各个卡口的车流量
  */
object Demo01StatisCarFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置连接kafka的配置信息
    val props = new Properties()
    //注意   sparkstreaming + kafka（0.10之前版本） receiver模式  zookeeper url（元数据）
    props.setProperty("bootstrap.servers", "node01:9092,node02:9092,node03:9092")
    props.setProperty("group.id", "flink-kafka-001")
    props.setProperty("key.deserializer", classOf[StringSerializer].getName)
    props.setProperty("value.deserializer", classOf[StringSerializer].getName)

    val stream = env.addSource(new FlinkKafkaConsumer[String]("flink-kafka", new SimpleStringSchema(), props))

    //monitorId + "\t").append(carId + "\t").append(timestamp + "\t").append(speed)
    stream.map(data => {
      val arr = data.split("\t")
      val monitorID = arr(0)
      (monitorID, 1)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(10))
      //      .reduce(new ReduceFunction[(String, Int)] {
      //        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) = {
      //          (value1._1, value1._2 + value2._2)
      //        }
      //      }).print()
      .aggregate(new AggregateFunction[(String, Int), Int, Int] {
      override def createAccumulator(): Int = 0

      override def add(value: (String, Int), acc: Int): Int = acc + value._2

      override def getResult(acc: Int): Int = acc

      override def merge(a: Int, b: Int): Int = a + b
    },
//      new WindowFunction[Int, (String, Int), String, TimeWindow] {
//      override def apply(key: String, window: TimeWindow, input: Iterable[Int], out: Collector[(String, Int)]): Unit = {
//        for (elem <- input) {
//          out.collect((key, elem))
//        }
//      }
//    }

    new ProcessWindowFunction[Int, (String, Int), String, TimeWindow] {
      override def process(key: String, context: Context, elements: Iterable[Int], out: Collector[(String, Int)]): Unit = {
        for (elem <- elements) {
          out.collect((key,elem))
        }
      }
    }
    ).print()
    env.execute()
  }
}
