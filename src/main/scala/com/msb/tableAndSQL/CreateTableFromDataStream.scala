package com.msb.tableAndSQL

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

case class Person(id:String,name:String,age:Int)

object CreateTableFromDataStream {

  def main(args: Array[String]): Unit = {
    //创建Table的上下文环境的第一套
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val stream: DataStream[Person] = streamEnv.socketTextStream("hadoop101", 9999)
      .map(line => {
        val arr: Array[String] = line.split(" ")
        new Person(arr(0).trim, arr(1).trim, arr(2).trim.toInt)
      })


//    tableEnv.fromDataStream(stream) //默认直接使用属性名字来作为字段名
    var table =tableEnv.fromDataStream(stream,'f_id,'f_name,'f_age) //Flink中字段表达式的写法，任何字段名前加'

    tableEnv.sqlQuery(s"select count(*) from ${table}")

    table.printSchema()

    tableEnv.execute("table_001") //都是启动流计算程序
//    streamEnv.execute()
  }
}
