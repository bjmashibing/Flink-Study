package com.msb.stream.sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object FileSInkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.socketTextStream("node01",8888)

    //<IN, BucketID>    IN:写入的数据的类型  BucketID：桶的名称的类型  日期+小时
    val rolling: DefaultRollingPolicy[String, String] = DefaultRollingPolicy.create()
      //文件5s钟没有写入新的数据，那么会产生一个新的小文件（滚动）
      .withInactivityInterval(5000)
      //当文件大小超过256M 也会滚动产生一个小文件
      .withMaxPartSize(256 * 1024 * 1024)
      //文件打开时间 超过10s 也会滚动产生一个小文件
      .withRolloverInterval(10000).build()


    val sink: StreamingFileSink[String] = StreamingFileSink
      .forRowFormat(new Path("c:/flink/data"), new SimpleStringEncoder[String]("UTF-8"))
      //每隔一段时间 检测以下桶中的数据  是否需要回滚
      .withBucketCheckInterval(100)
      //设置桶中的小文件的滚动策略：文件大小、文件打开时间、文件的不活跃时间
      .withRollingPolicy(rolling)
      .build()

    stream.addSink(sink)
    env.execute()

  }
}
