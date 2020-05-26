package com.msb.table

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala._

object WordCount {
  //自定义UDF
  class MyFlatMapFunction extends TableFunction[Row]{
    //定义类型
    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING, Types.INT)
    }
    //函数主体
    def eval(str:String):Unit ={
      str.trim.split(" ")
        .foreach({word=>{
          var row =new Row(2)
          row.setField(0,word)
          row.setField(1,1)
          collect(row)
        }})
    }
  }

  def main(args: Array[String]): Unit = {

    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    //初始化Table API的上下文环境
    val tableEvn =StreamTableEnvironment.create(streamEnv)
    val stream: DataStream[String] = streamEnv.socketTextStream("node01",8888)
    val table: Table = tableEvn.fromDataStream(stream,'words)
    var my_func =new MyFlatMapFunction()//自定义UDF
    val result: Table = table.flatMap(my_func('words)).as('word, 'count)
      .groupBy('word) //分组
      .select('word, 'count.sum as 'c) //聚合

    //更新的数据为true,未更新为false
    tableEvn.toAppendStream[Row](result)
      .print()
    tableEvn.execute("table_api")

  }
}

