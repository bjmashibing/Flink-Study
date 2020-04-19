package com.msb.stream.transformation

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

object MapOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1 to 100
//    val stream = env.generateSequence(1,100)
    val stream = env.socketTextStream("node01",8888)

    stream.map(x=>x+"-------")
    //如何使用flatMap 代替 filter
    //数据中包含了abc 那么这条数据就过滤掉  flatMap
    //flatMap算子：map flat(扁平化)   flatMap 集合
    /*stream.flatMap(x => {
      val rest = new ListBuffer[String]
      if(!x.contains("abc")){
        rest += x
      }
      rest.iterator
    }).print()*/

    //keyBy算子：分流算子  根据用户指定的字段来分组
    stream
      .flatMap(_.split(" "))
        .map((_,1))
//        .keyBy(new KeySelector[(String,Int),String] {
//          override def getKey(value: (String, Int)): String = {
//            value._1
//          }
//        })
      .keyBy(x=>x._1)
      //flink 去重  怎么做？  流计算
      .reduce((v1,v2) => (v1._1,v1._2+v2._2))
      .print()
    env.execute()
  }
}
