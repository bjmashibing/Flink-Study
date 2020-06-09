package com.msb.tableAndSQL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object TestFlinkSQL {

  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val stream: DataStream[StationLog] = streamEnv.socketTextStream("hadoop101", 9999)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new StationLog(arr(0).trim, arr(1).trim, arr(2).trim,arr(3).trim,arr(4).trim.toLong,arr(5).trim.toLong)
      }) .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(2)) {
      override def extractTimestamp(t: StationLog) = t.callTime
    })

    val table: Table = tableEnv.fromDataStream(stream,'sid,'callOut,'callIn,'callType,'callTime.rowtime,'duration)
//    tableEnv.registerDataStream("t_station",stream)
    //1、统计每个基站的通话次数，而且是通话成功的
//    val result: Table = tableEnv.sqlQuery("select sid,count(1) from t_station where callType='success' group by sid ")

    //2、每隔5秒，每个基站通话成功的通话时长总和
//    val result: Table = tableEnv.sqlQuery(s"  select sid,sum(duration),tumble_start(callTime,interval '5' second),tumble_end(callTime,interval '5' second) from ${table}  where callType='success' " +
//      s"group by tumble(callTime,interval '5' second),sid")

    //3、每隔5秒，统计每个基站，最近10秒内，所有通话成功的通话时长总和。窗口长度是10s,滑动步长是5s
    val result: Table = tableEnv.sqlQuery(s"  select sid,sum(duration),hop_start(callTime,interval '5' second,interval '10' second),hop_end(callTime,interval '5' second,interval '10' second) from ${table}  where callType='success' " +
      s"group by hop(callTime,interval '5' second,interval '10' second),sid")
    //hop(三个参数)：第一个参数：时间字段,第二个：滑动步长，第三个:窗口长度
    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()

    tableEnv.execute("job3")
  }
}
