package com.msb.tableAndSQL

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row


/**
 *
 * @param sid 基站编号
 * @param callOut
 * @param callIn
 * @param callType
 * @param callTime
 * @param duration
 */
case class StationLog(sid:String,callOut:String,callIn:String,callType:String,callTime:Long,duration:Long)
object TestTableAPI {

  def main(args: Array[String]): Unit = {
    //创建Table的上下文环境的第一套
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      })

    //统计每个基站的通话数量
    val table: Table = tableEnv.fromDataStream(stream)


//    val result: Table = table.groupBy('sid) //group by 是有状态的算子
//      .select('sid, 'sid.count)

    val result: Table = table.where('callType==="success")
      .select('sid, 'callOut, 'callIn, 'callType)



    //table对象中的数据输出，必须要转换成DataStream，调用sink
//    val resultStream: DataStream[Row] = result.toAppendStream[Row]
//    resultStream.print()
    //toAppendStream方法，如果状态中的数据没有insert或者update，就可以使用
    //第二种

    tableEnv.toRetractStream[Row](result) //都可以使用
//      .filter(_._1==true) //不加也行，把状态中被修改的内容不要写入数据流中 ,状态中数据修改之后的结果数据为true，原来的old数据为false
      .print()

    tableEnv.execute("job1")
  }
}
