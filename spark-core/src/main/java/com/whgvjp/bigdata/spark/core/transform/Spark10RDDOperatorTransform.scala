package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark10RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(1,2,3,4,5,6),3)
    val newRDD = rdd.coalesce(2,true)


    newRDD.saveAsTextFile("output")


    sc.stop()
  }

}
