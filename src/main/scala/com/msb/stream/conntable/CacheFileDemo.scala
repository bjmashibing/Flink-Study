package com.msb.stream.conntable

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.FileUtils

import scala.collection.mutable

object CacheFileDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //将文件注册到env,同时取一个名字
    env.registerCachedFile("/root/id2city","id2city")

    //id 001 002
    val socketStream = env.socketTextStream("node01",8888)
    val stream = socketStream.map(_.toInt)
    //RichMapFunction 通过富函数类的方式来使用map算子
    //MapFunction
    stream.map(new RichMapFunction[Int,String] {
      private val id2CityMap = new mutable.HashMap[Int,String]()

      //每一个thread只会调用一次
      override def open(parameters: Configuration): Unit = {
        val file = getRuntimeContext().getDistributedCache().getFile("id2city")
        val str = FileUtils.readFileUtf8(file)
        val strings = str.split("\r\n")
        for(str <- strings){
          val splits = str.split(" ")
          val id = splits(0).toInt
          val city = splits(1)
          id2CityMap.put(id,city)
        }
      }
      override def map(key: Int): String = {
        id2CityMap.getOrElse(key,"not found city")
      }
    }).print()
    env.execute()
  }
}
