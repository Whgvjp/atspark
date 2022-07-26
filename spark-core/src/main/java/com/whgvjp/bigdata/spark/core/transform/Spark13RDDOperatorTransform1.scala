package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13RDDOperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd1 = sc.makeRDD(Seq(1,2,2,3,3,4),4)
    val rdd2 = sc.makeRDD(Seq(3,4,5,6,7,7),4)

    val rdd3 = rdd1.zip(rdd2)
    println(rdd3.collect().mkString(","))


    sc.stop()
  }

}
