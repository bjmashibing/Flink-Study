package com.msb.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
/**
  * 从DataStream中创建Table（动态表）
  */
object Test02 {
  def main(args: Array[String]): Unit = {
    //创建流式计算的上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建Table API的上下文环境
    val tableEvn = StreamTableEnvironment.create(env)

    val stream = env.socketTextStream("node01", 8888)
      .map(line => {
        val elems = line.split(" ")
        (elems(0).toInt, elems(1), elems(2).toDouble)
      })

//    tableEvn.registerDataStream("table",stream)
//    tableEvn.registerDataStream("table",stream,'id,'name,'score)
//    tableEvn.scan("table").printSchema()

//    val table = tableEvn.fromDataStream(stream)
    val table = tableEvn.fromDataStream(stream,'id,'name,'score)
    table.printSchema()

//    查询
//    tableEvn
//      .toAppendStream[Row](table.select('id,'name))
//        .print()

//过滤查询
    tableEvn.toAppendStream[Row](table.filter('id===1).filter('score > 60)).print()

    env.execute()
  }
}
