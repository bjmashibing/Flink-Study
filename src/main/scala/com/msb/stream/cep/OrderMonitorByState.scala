package com.msb.stream.cep

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


case class OrderInfo(oid:String,status:String,payId:String,actionTime:Long)

//提示信息的样例类
case class OrderMessage(oid:String,msg:String,createTime:Long,payTime:Long)
//第一次使用状态编程
object OrderMonitorByState {

  //在京东里面，一个订单创建之后，15分钟内如果没有支付，会发送一个提示信息给用户，
  // 如果15分钟内已经支付的，需要发一个提示信息给商家
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._

    //创建一个侧输出流的Tag
    val tag =new OutputTag[OrderMessage]("pay_timeout")

    val stream: DataStream[OrderInfo] = env.readTextFile(getClass.getResource("/OrderLog.csv").getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new OrderInfo(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignAscendingTimestamps(_.actionTime * 1000L)

    val mainStream: DataStream[OrderMessage] = stream.keyBy(_.oid)
      .process(new OrderProcess(tag))

    mainStream.getSideOutput(tag).print("侧流")
    mainStream.print("主流")

    env.execute()

  }

  class OrderProcess(tag:OutputTag[OrderMessage]) extends KeyedProcessFunction[String,OrderInfo,OrderMessage]{
    //状态直接保存订单创建的数据
    var createOrderState:ValueState[OrderInfo] =_

    //状态中还必须保存触发时间
    var timeOutState:ValueState[Long]=_


    override def open(parameters: Configuration): Unit = {
      //初始化状态
      createOrderState =getRuntimeContext.getState(new ValueStateDescriptor[OrderInfo]("create_order",classOf[OrderInfo]))

      timeOutState =getRuntimeContext.getState(new ValueStateDescriptor[Long]("time_out",classOf[Long]))
    }

    override def processElement(value: OrderInfo, ctx: KeyedProcessFunction[String, OrderInfo, OrderMessage]#Context, out: Collector[OrderMessage]): Unit = {
      //首先从状态中取得订单的创建数据
      val createOrder: OrderInfo = createOrderState.value()
      if(value.status.equals("create") &&  createOrder==null){ //刚刚开始创建好订单
        createOrderState.update(value) //把刚刚创建的订单信息存入状态
        var ts =value.actionTime*1000L + (15*60*1000) //提示信息的触发器  触发的时间
        timeOutState.update(ts)
        //开始注册触发器
        ctx.timerService().registerEventTimeTimer(ts)
      }

      if(value.status.equals("pay") && createOrder!=null){ //当前订单创建了，并且支付了
        //在判断一下支付是否超时
        if(timeOutState.value()>value.actionTime*1000L){ //支付没有超时
          //先把触发器删除
          ctx.timerService().deleteEventTimeTimer(timeOutState.value())
          //生成提示信息给商家
          var om =new OrderMessage(value.oid,"订单正常支付，请尽快发货!",createOrder.actionTime,value.actionTime)
          out.collect(om) //发送到主流
          //清理状态
          timeOutState.clear()
          createOrderState.clear()
        }
      }
    }

    //触发器 触发的方法
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, OrderInfo, OrderMessage]#OnTimerContext, out: Collector[OrderMessage]): Unit = {
      val createOrder: OrderInfo = createOrderState.value
      if(createOrder!=null){ //触发器 开始触发了，订单创建之后过了15分钟，需要给用户一个提示信息 ,用户的提示信息放入侧流中
        ctx.output(tag,new OrderMessage(createOrder.oid," 该订单在15分钟内没有支付 请尽快支付",createOrder.actionTime,0))
        //清理状态
        createOrderState.clear()
        timeOutState.clear()
      }
    }
  }
}
