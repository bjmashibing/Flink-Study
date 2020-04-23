package com.msb.stream.util

import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

import scala.collection.mutable.ListBuffer

object HBaseUtil {


  /**
    * 设置HBaseConfiguration
    *
    *
    */
  def getHBaseConfiguration() = {
    val prop = new Properties()
    val inputStream = HBaseUtil.getClass.getClassLoader.getResourceAsStream("hbase.properties")

    prop.load(inputStream)

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", prop.getProperty("hbase.zookeeper.quorum"))
    conf.set("hbase.zookeeper.property.clientPort", prop.getProperty("hbase.zookeeper.property.clientPort"))
    conf
  }

  def getConn(tableName: String) = {
    val conf = HBaseUtil.getHBaseConfiguration()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    ConnectionFactory.createConnection(conf)
  }

  /**
    * 通过RowKey获取某张表的数据
    *
    * @param tableName
    * @param rowKey
    */
  def getRecord(tableName: String, rowKey: String, conn: Connection) = {
    val conn = getConn(tableName)
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)

    val list = new ListBuffer[String]()
    for (rowKv <- result.rawCells()) {
      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
      println("TimeStamp:" + rowKv.getTimestamp)
      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      println("Value:" + value)
      if (value.length > 0)
        list.++=(value.split("\\|"))
    }
    list
  }

  def getRecord1(tableName: String, rowKey: String) = {
    val conn = getConn(tableName)
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)

    val list = new ListBuffer[String]()
    for (rowKv <- result.rawCells()) {
      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      println("Qualifier:" + new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8"))
      println("TimeStamp:" + rowKv.getTimestamp)
      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      println("Value:" + value)
      if (value.length > 0)
        list.++=(value.split("\\|"))
    }
    list
  }

  def getRecord2(tableName: String, rowKey: String) = {
    val conn = getConn(tableName)
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)

    val list = new ListBuffer[String]()
    for (rowKv <- result.rawCells()) {
      //      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      //      println("Qualifier:" + )
      val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")

      //      println("TimeStamp:" + rowKv.getTimestamp)
      //      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      //      println("Value:" + value)
      if (colName.length > 0)
        list.+=(colName)
    }
    list
  }

  def getRecord3(tableName: String, rowKey: String, conn: Connection) = {
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(rowKey))
    val result: Result = table.get(get)

    val list = new ListBuffer[String]()
    for (rowKv <- result.rawCells()) {
      //      println("Famiily:" + new String(rowKv.getFamilyArray, rowKv.getFamilyOffset, rowKv.getFamilyLength, "UTF-8"))
      //      println("Qualifier:" + )
      val colName = new String(rowKv.getQualifierArray, rowKv.getQualifierOffset, rowKv.getQualifierLength, "UTF-8")

      //      println("TimeStamp:" + rowKv.getTimestamp)
      //      println("rowkey:" + new String(rowKv.getRowArray, rowKv.getRowOffset, rowKv.getRowLength, "UTF-8"))
      val value = new String(rowKv.getValueArray, rowKv.getValueOffset, rowKv.getValueLength, "UTF-8")
      //      println("Value:" + value)
      if (colName.length > 0)
        list.+=(colName)
    }
    list
  }

  /**
    * 返回或新建HBaseAdmin
    *
    * @param conf
    * @param tableName
    * @return
    */
  def getHBaseAdmin(conf: Configuration, tableName: String) = {
    val admin = new HBaseAdmin(conf)
    if (!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }

    admin
  }

  /**
    * 返回HTable
    *
    * @param tableName
    * @return
    */
  def getTable(conf: Configuration, tableName: String) = {
    //    val conf = HBaseUtil.getHBaseConfiguration()
    //    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    new HTable(conf, tableName)
  }

  def getUserProfileTable(tableName: String) = {
    val conf = HBaseUtil.getHBaseConfiguration()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    new HTable(conf, tableName)
  }


  def main(args: Array[String]): Unit = {
    val conf = getHBaseConfiguration()
     val conn = ConnectionFactory.createConnection(conf)
     //    println(getRecord("article_similar","337747",conn))
     val htable = HBaseUtil.getTable(conf, "program_similar")
     val put = new Put(Bytes.toBytes("1"))
     put.addColumn(Bytes.toBytes("similar"), Bytes.toBytes("12"), Bytes.toBytes("11.0"))
     htable.put(put)

//    getRecord2("program_similar", "90107").foreach(println)
  }


  def getConf(tableName: String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", PropertiesUtils.getProp("hbase.zookeeper.property.clientPort"))
    conf.set("hbase.zookeeper.quorum", PropertiesUtils.getProp("hbase.zookeeper.quorum"))
    conf.set("zookeeper.znode.parent", PropertiesUtils.getProp("zookeeper.znode.parent"))
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf
  }
}
