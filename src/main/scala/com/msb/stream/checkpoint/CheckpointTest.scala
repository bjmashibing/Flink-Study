package com.msb.stream.checkpoint

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CheckpointTest {
  def main(args: Array[String]): Unit = {

    val  env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List(1,2,3,4,5,6,7,8,9))

    stream.keyBy(x=>{
      x % 2
    }).reduce((v1:Int,v2:Int)=>v1+v2)
      .print()

    env.execute()


  }
}
