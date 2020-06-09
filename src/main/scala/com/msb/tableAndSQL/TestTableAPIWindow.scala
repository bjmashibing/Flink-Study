package com.msb.tableAndSQL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object TestTableAPIWindow {

  //每隔5秒，统计每个基站中通话成功的数量,假设数据基于呼叫时间乱序。
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
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StationLog](Time.seconds(2)) {
        override def extractTimestamp(t: StationLog) = t.callTime
      })

    val table: Table = tableEnv.fromDataStream(stream,'sid,'callOut,'callIn,'callType,'callTime.rowtime)

    val result: Table = table.filter('callType === "success")
      .window(Tumble.over("5.second").on("callTime").as("win"))
      .groupBy('win, 'sid)
      .select('sid, 'sid.count, 'win.start, 'win.end)

    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()

    tableEnv.execute("job3")
  }
}
