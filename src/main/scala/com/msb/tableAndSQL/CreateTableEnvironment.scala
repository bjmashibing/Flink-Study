package com.msb.tableAndSQL

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

object CreateTableEnvironment {

  def main(args: Array[String]): Unit = {
    //创建Table的上下文环境的第一套
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)

    //需要两个隐式转换
//    import import org.apache.flink.streaming.api.scala._
//    import org.apache.flink.table.api.scala._


    //第二套API
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner() //是否使用阿里巴巴的BlinkAPI
      .inStreamingMode() //是否用于流计算
      .build()

    StreamTableEnvironment.create(streamEnv,settings)
  }
}
