package com.msb.stream.source

import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.source.FileProcessingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
  * flume->hdfs->flink 实时ETL->入仓
  *
  * flume->kafka->flink 实时ETL->入仓
  */
object ReadHDfS {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    val stream: DataStream[String] = env.readTextFile("hdfs://node01:9000/flink/data/wc")
    //    val filePath = "./data/carId2Name"
    val filePath = "hdfs://node01:9000/flink/data/"
    val format = new TextInputFormat(new Path(filePath))

    //readFile  可以设置每隔一段时间去监控某一个目录  或者某一个文件内容
    try {
      val stream = env.readFile(format, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 10)
      stream.print()
    } catch {
      case ex: Exception => {
        ex.getMessage()
      }
    }
    env.execute()
  }
}
