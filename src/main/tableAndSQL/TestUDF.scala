package com.msb.tableAndSQL

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{Table, Types}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TestUDF {

  def main(args: Array[String]): Unit = {
    //创建Table的上下文环境的第一套
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)
    import org.apache.flink.streaming.api.scala._
    import org.apache.flink.table.api.scala._
    val stream: DataStream[String] = streamEnv.socketTextStream("hadoop101", 9999)

    val table: Table = tableEnv.fromDataStream(stream,'line)

    //创建一个UDF
    val my_udf = new MyUDFFunction

    val result: Table = table.flatMap(my_udf('line)).as('word, 'word_count)
      .groupBy('word)
      .select('word, 'word_count.sum as 'abc)

    tableEnv.toRetractStream[Row](result)
      .filter(_._1==true)
      .print()

    tableEnv.execute("job2")


  }

  //自定义的UDF
  class  MyUDFFunction extends TableFunction[Row] {
    //定义UDF的返回字段类型,单词作为第一个字段，单词的个数作为第二个字段
    override def getResultType: TypeInformation[Row] = {
      Types.ROW(Types.STRING(),Types.INT())
    }

    def eval(line:String) ={
      line.split(" ").foreach(word=>{
        val row = new Row(2)
        row.setField(0,word)
        row.setField(1,1)
        collect(row) //把新增Row输出到数据流
      })
    }
  }
}
