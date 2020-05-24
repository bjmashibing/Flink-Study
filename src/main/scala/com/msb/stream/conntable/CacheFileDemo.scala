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
    env.registerCachedFile("/root/id2city","id2city")

    val socketStream = env.socketTextStream("node01",8888)
    val stream = socketStream.map(_.toInt)
    stream.map(new RichMapFunction[Int,String] {

      private val id2CityMap = new mutable.HashMap[Int,String]()
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
      override def map(value: Int): String = {
        id2CityMap.getOrElse(value,"not found city")
      }
    }).print()
    env.execute()
  }
}
