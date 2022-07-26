package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark09RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))
    val rdd1 = rdd.distinct()
    rdd1.collect().foreach(println)

//    List(1,1,2,2).distinct.foreach(println)

    sc.stop()
  }

}
