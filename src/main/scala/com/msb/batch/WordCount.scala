package com.msb.batch

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val initDS: DataSet[String] = env.readTextFile("hdfs://node01:9000/flink/data/wc")
    val restDS: AggregateDataSet[(String, Int)] = initDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    restDS.print()
  }
}
