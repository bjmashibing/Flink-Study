package com.msb.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

//车辆每经过5个卡口，统计这辆车的平均速度
object TumblingCountWindowKeyed03 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)
    stream
      .map(x=>{
        val splits = x.split(" ")
        (splits(0),splits(1).toLong)
      })
      .keyBy(x => x._1)
      //当窗口内相同key的元素数大于等于5则触发窗口执行，窗口计算时 只是计算key对应元素超过5的这批数据，其他数据不计算
      .countWindow(5)
      .reduce(new ReduceFunction[(String, Long)] {
        override def reduce(value1: (String, Long), value2: (String, Long)): (String, Long) = {
          (value1._1,value1._2+value2._2)
        }
      },new WindowFunction[(String,Long),(String,Double),String,GlobalWindow] {
        override def apply(key: String, window: GlobalWindow, input: Iterable[(String, Long)], out: Collector[(String, Double)]): Unit = {
          out.collect((key,input.head._2/5.0))
        }
      })
      .print()
    env.execute()
  }
}
