package com.msb.stream.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import scala.collection.mutable.ListBuffer

/**
  * 将WordCount的计算结果写入到Redis中
  * 注意：Flink是一个流式计算框架   hello 1   hello 3
  * redis存数据的需要是幂等操作
  * hello 1
  * hello 2
  * redis   hset（word，int）
  * hbase
  *
  */
object RedisSinkTest {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("node01",8888)


    val jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node01").setPort(6379).setDatabase(3).build()

    stream.flatMap(line => {
        val rest = new ListBuffer[(String,Int)]
       line.split(" ").foreach(word => rest += ((word,1)))
      rest
    }).keyBy(_._1)
      .reduce((v1:(String,Int),v2:(String,Int))=>{
        (v1._1,v1._2+v2._2)
      }).addSink(new RedisSink(jedisPoolConfig,new RedisMapper[(String,Int)] {
      //指定操作Redis的命令
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET,"wc")
      }

      override def getKeyFromData(data: (String, Int)): String = {
        data._1
      }

      override def getValueFromData(data: (String, Int)): String = {
        data._2 + ""
      }
    }))
    env.execute()

  }
}
