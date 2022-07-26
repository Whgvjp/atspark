package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDOperatorTransformTest1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    //TODO 算子 -map

    // 1. numSlices设置为1， 可以看出来RDD的计算一个分区内的数据是一个一个执行逻辑
    // 只有前面一个数据的全部逻辑执行完毕之后，才会执行下一个数据
    // 2. numSlices 设置为2， 分区内的数据是有序的
    // 不同分区的数据计算是无需的
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4),2)
    val mapRDD = rdd.map(
      num => {
        println(">>>>>> " + num)
        num
      }
    )
    val mapRDD1 = mapRDD.map(
      num => {
        println("###### " + num)
        num
      }
    )

    mapRDD1.collect()


    sc.stop()
  }

}
