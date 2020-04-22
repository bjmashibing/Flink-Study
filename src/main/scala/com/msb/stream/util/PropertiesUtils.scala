package com.msb.stream.util

import java.util.Properties

object PropertiesUtils {
  val prop = new Properties()
  val inputStream = PropertiesUtils.getClass.getClassLoader.getResourceAsStream("spark-conf.properties")
  prop.load(inputStream)

  def getProp(name:String): String ={
     prop.getProperty(name)
  }

  def main(args: Array[String]): Unit = {
   println( PropertiesUtils.getProp("spark.streaming.app.name"))
  }
}
