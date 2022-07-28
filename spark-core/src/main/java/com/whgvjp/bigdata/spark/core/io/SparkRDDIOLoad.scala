package com.whgvjp.bigdata.spark.core.io

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDIOLoad {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.textFile("output1")
    println(rdd1.collect.mkString(","))

    val rdd2 = sc.objectFile[(String,Int)]("output2")
    println(rdd2.collect.mkString(","))

    val rdd3 = sc.sequenceFile[String,Int]("output3")
    println(rdd3.collect.mkString(","))
    //    rdd.saveAsTextFile("output1")
//    rdd.saveAsObjectFile("output2")
//    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
