package com.msb.batch

import org.apache.flink.api.java.aggregation.Aggregations
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object ReduceGroupOperator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds: DataSet[(String, Int)] = env.fromCollection(List(("A",1),("A",2),("A",3),("B",9),("B",1)))
    ds.groupBy(0)
      .reduceGroup(item =>{
        var count = 0
        var key =""
        for (elem <- item) {
          count += elem._2
          key = elem._1
        }
        (key,count)
      }).print()

    ds.groupBy(0)
      .aggregate(Aggregations.MIN,1)
      .print()

  }
}
