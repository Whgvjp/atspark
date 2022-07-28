package com.whgvjp.bigdata.spark.core.io

import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDIOSave {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd = sc.parallelize(
      List(
        ("a",1),
        ("b",2),
        ("c",3)
      )
    )

    rdd.saveAsTextFile("output1")
    rdd.saveAsObjectFile("output2")
    rdd.saveAsSequenceFile("output3")

    sc.stop()
  }
}
