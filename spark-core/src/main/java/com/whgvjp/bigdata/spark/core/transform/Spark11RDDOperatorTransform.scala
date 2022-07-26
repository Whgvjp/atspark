package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark11RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),2)
    // 扩大分区
//    val newRDD = rdd.coalesce(3,true)

    //另一种
    val newRDD = rdd.repartition(3)

    newRDD.saveAsTextFile("output")


    sc.stop()
  }

}
