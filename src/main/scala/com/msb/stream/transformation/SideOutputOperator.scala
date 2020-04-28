package com.msb.stream.transformation

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object SideOutputOperator {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)
    //定义测输出流的tag标签
    val gtTag = new OutputTag[String]("gt")
    //process  理解成是flink一个算子    process 底层API
    val processStream = stream.process(new ProcessFunction[String, String] {
      //处理每一个元素
      override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
        try {
          val longVar = value.toLong
          if (longVar > 100) {
            //往主流发射
            out.collect(value)
          } else {
            //往测流发射
            ctx.output(gtTag, value)
          }
        } catch {
          case e => e.printStackTrace()
            ctx.output(gtTag, value)
        }
      }
    })
    //获取测流数据
    val sideStream = processStream.getSideOutput(gtTag)
    sideStream.print("sideStream")
    //默认情况 print打印的是主流数据
    processStream.print("mainStream")
    env.execute()



  }
}
