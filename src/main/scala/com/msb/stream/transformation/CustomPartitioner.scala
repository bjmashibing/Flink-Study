package com.msb.stream.transformation

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object CustomPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val stream = env.generateSequence(1,10).map((_,1))
    stream.writeAsText("./data/stream1")
    //根据哪个字段来分区
    //partitionCustom（partitoner：自定义的partitioner，指定进行分区的字段）
    stream.partitionCustom(new customPartitioner(),0)
      .writeAsText("./data/stream2").setParallelism(4)
    env.execute()
  }

  class customPartitioner extends Partitioner[Long]{
    override def partition(key: Long, numPartitions: Int): Int = {
      key.toInt % numPartitions
    }
  }
}
