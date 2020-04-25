package com.msb.stream.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.api.scala._

import scala.collection.mutable.ListBuffer

/***
  * wordCount结果写入到MySQL中
  *
  * mySQL天生不支持幂等操作
  *代码实现幂等操作
  *
  * flink 1
  * flink 2   -> flink3 (update   insert)
  * flink 3
  *
  * MySQL不是flink内嵌支持的  所以需要自定义sink
  * 导入MySQL驱动包
  */
object MySQlSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)

    val stream = env.socketTextStream("node01",8888)

    val jedisPoolConfig = new FlinkJedisPoolConfig.Builder().setHost("node01").setPort(6379).setDatabase(3).build()

    stream.flatMap(line => {
      val rest = new ListBuffer[(String,Int)]
      line.split(" ").foreach(word => rest += ((word,1)))
      rest
    }).keyBy(_._1)
      .reduce((v1:(String,Int),v2:(String,Int))=>{
        (v1._1,v1._2+v2._2)
      }).addSink(new RichSinkFunction[(String, Int)] {
      //在线程开始指定业务逻辑之前会调用一次 一次 一次

      var conn:Connection = _
      var updatePst:PreparedStatement = _
      var insertPst:PreparedStatement = _
      //定义一个集合

      //
      override def open(parameters: Configuration): Unit = {
        conn = DriverManager.getConnection("jdbc:mysql://node01:3306/test","root","123123")
        updatePst = conn.prepareStatement("update wc set count = ? where word = ?")
        insertPst = conn.prepareStatement("insert into wc values(?,?)")
      }

      //正常业务指定完毕，会被调用一次一次一次
      override def close(): Unit = {
        updatePst.close()
        insertPst.close()
        conn.close()
      }

      //每有一个元素过来  会被调用一次  foreachpartiton
      override def invoke(value: (String, Int), context: SinkFunction.Context[_]): Unit = {
        updatePst.setInt(1,value._2)
        updatePst.setString(2,value._1)
        updatePst.execute()
        if(updatePst.getUpdateCount == 0){
          insertPst.setString(1,value._1)
          insertPst.setInt(2,value._2)
          insertPst.execute()
        }
      }
    })

    env.execute()
  }
}
