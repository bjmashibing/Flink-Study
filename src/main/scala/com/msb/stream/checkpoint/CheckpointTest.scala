package com.msb.stream.checkpoint

import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * 基于wordCount代码来测试checkpoint
  *
  * 开启checkpoint
  */
object CheckpointTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置状态后端  所谓状态后端就是数据保存位置
    env.setStateBackend(new FsStateBackend("hdfs://node01:9000/flink/checkpoint",true))

    //代表每隔1000ms 往数据源中插入一个barrier
    env.enableCheckpointing(1000)
    //设置容许checkpoint失败的次数
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)
    //checkpoint超时时间  10分钟
    env.getCheckpointConfig.setCheckpointTimeout(5 * 60 * 1000)
    //设置checkpoint模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    /**
      * 设置checkpoint任务之间的间隔时间  checkpoint job1  checkpoint job2
      * 防止触发太密集的flink checkpoint，导致消耗过多的flinl集群资源  导致影响整体性能
      */
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(600)
    //设置checkpoint最大并行的个数   3checkpoint job
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //flink 任务取消之后，checkpoint数据是否删除
    // RETAIN_ON_CANCELLATION 当任务取消，checkpoints数据会保留
    // DELETE_ON_CANCELLATION 当任务取消，checkpoints数据会删除
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val stream = env.socketTextStream("node01", 8888)

    stream.flatMap(_.split(" "))
      .filter(!"asd".equals(_))
      .map((_, 1)).uid("map")
      .keyBy(x => x._1)
      .reduce((v1: (String, Int), v2: (String, Int)) => {
        (v1._1, v1._2 + v2._2)
      }).uid("reduce")
      .print()
    env.execute()
  }
}
