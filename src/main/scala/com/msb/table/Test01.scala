package com.msb.table

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * 从文件中创建Table（静态表）
  */
object Test01 {
  def main(args: Array[String]): Unit = {
    //创建流式计算的上下文环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //创建Table API的上下文环境
    val tableEvn = StreamTableEnvironment.create(env)


    val source = new CsvTableSource("D:\\code\\StudyFlink\\data\\tableexamples"
      , Array[String]("id", "name", "score")
      , Array(Types.INT, Types.STRING, Types.DOUBLE)
    )
    //将source注册成一张表  别名：exampleTab
    tableEvn.registerTableSource("exampleTab",source)
    tableEvn.scan("exampleTab").printSchema()

  }
}
