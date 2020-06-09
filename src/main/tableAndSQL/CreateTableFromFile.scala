package com.msb.tableAndSQL

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

object CreateTableFromFile {

  def main(args: Array[String]): Unit ={

    //创建Table的上下文环境的第一套
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)

    val tableSource = new CsvTableSource(
      getClass.getResource("/../../../data/carFlow_all_column_test.txt").getPath,
      Array[String]("f1", "f2", "f3", "f4", "f5", "f6"),
      Array(Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.LONG, Types.INT)
    )

    //注册一张表
   tableEnv.registerTableSource("t_station",tableSource)

    //根据元数据还原一张表
    val table: Table = tableEnv.scan("t_station")

    table.printSchema() //打印表结构




  }
}
