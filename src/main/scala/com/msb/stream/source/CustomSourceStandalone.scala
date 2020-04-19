package com.msb.stream.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.util.Random

//自定义数据源
object CustomSourceStandalone {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //[String] 发射数据类型
    //如果是基于SourceFunction接口实现自定义数据源，这个支持单并行度
    //Source: 1 is not a parallel source
    val stream = env.addSource(new SourceFunction[String] {
      var flag = true

      //发射数据
      override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
        //run  读取任何地方数据，然后将数据 发射出去  redis
        val random = new Random()
        while (flag) {
          ctx.collect("hello" + random.nextInt(1000))
          Thread.sleep(500)
        }
      }
      //停止
      override def cancel(): Unit = {
        flag = false
      }
    }).setParallelism(2)
    stream.print()
    env.execute()

  }
}
