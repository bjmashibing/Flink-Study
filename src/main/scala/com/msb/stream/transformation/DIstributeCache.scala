package com.msb.stream.transformation

import java.io.File
import java.util

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * 分布式缓存， Flink提供了分布式缓存，再处理数据的时候，可以使用分布式缓存的数据
  * 分布式缓存的工作机制：为程序注册一个文件（本地，HDFS），当FLink程序启动的时候
  * 会自动将这个文件分发到TaskManager节点的工作目录区,subTask再执行的时候就可以使用到这个文件
  */
object DIstributeCache {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.registerCachedFile("/root/carId2Name","carId2NameFile")

    val stream = env.socketTextStream("node01",8888)
    stream.map(new RichMapFunction[String,String] {
            private val map = new mutable.HashMap[String,String]()
            //使用文件
            override def open(parameters: Configuration): Unit = {
              val carId2NameFile: File = getRuntimeContext.getDistributedCache.getFile("carId2NameFile")
              val lines: util.List[String] = FileUtils.readLines(carId2NameFile)
              val iterator: util.Iterator[String] = lines.iterator()
              while(iterator.hasNext){
                val line = iterator.next()
                val splits = line.split(" ")
                map.put(splits(0),splits(1))
        }
      }

      override def map(key: String): String = {
        val value:String = map.getOrElse(key,"not found name")
        value
      }


    }).print()
    env.execute()
  }
}
