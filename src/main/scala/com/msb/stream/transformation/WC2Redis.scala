package com.msb.stream.transformation

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis


//将wordcount的结果写入到redis中
object WC2Redis {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01", 8888)

    val restStream = stream.flatMap(_.split(" ")).map((_,1)).keyBy(0).sum(1)

    restStream.map(new RichMapFunction[(String,Int),String] {


      var jedis: Jedis = _
      //在subtask启动的时候，首先调用的方法  redis3号数据库
      override def open(parameters: Configuration): Unit = {
        //Task的名字
        val name = getRuntimeContext.getTaskName
        //获取子任务的名字
        val nameWithSubtasks = getRuntimeContext.getTaskNameWithSubtasks
        println("name:" + name + "\t subtask name:" + nameWithSubtasks)

        jedis = new Jedis("node01",6379)
        jedis.select(3)
      }

      //处理每一个元素的
      override def map(value: (String, Int)): String = {
        jedis.set(value._1,value._2+"")
        value._1
      }


      //在subtask执行完毕前的时候，调用的方法
      override def close(): Unit = {
        jedis.close()
      }

    })

    env.execute()
  }
}
