package com.msb.stream.transformation

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable

/**
  * 现有一个配置文件存储车牌号与车主的真实姓名，\
  * 通过数据流中的车牌号实时匹配出对应的车主姓名
  * （注意：配置文件可能实时改变）
  * 配置文件可能实时改变  读取配置文件的适合  readFile  readTextFile（读一次）
  * stream1.connect(stream2)
  */
object CoFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(3)
    val filePath = "data/carId2Name"
    val carId2NameStream = env.readFile(new TextInputFormat(new Path(filePath)),filePath,FileProcessingMode.PROCESS_CONTINUOUSLY,10)
    val dataStream = env.socketTextStream("node01",8888)
    dataStream.connect(carId2NameStream).map(new CoMapFunction[String,String,String] {
//      每一个thread都会保存一个hashMap集合  不建议
      private val hashMap = new mutable.HashMap[String,String]()
      override def map1(value: String): String = {
        hashMap.getOrElse(value,"not found name")
      }

      override def map2(value: String): String = {
        val splits = value.split(" ")
        hashMap.put(splits(0),splits(1))
        value + "加载完毕..."
      }
    }).print()
    env.execute()

    env.execute()
  }
}
