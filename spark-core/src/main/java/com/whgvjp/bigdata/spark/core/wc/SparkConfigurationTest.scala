package com.whgvjp.bigdata.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfigurationTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkConfigurationTest").setMaster("local").set("spark.port.maxRetries","200")
    val sc = new SparkContext(conf)
    sc.getConf.getAll.foreach(println)
    sc.stop()
  }
}
