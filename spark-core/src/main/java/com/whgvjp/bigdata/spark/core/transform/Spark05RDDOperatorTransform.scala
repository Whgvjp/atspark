package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    glomRDD.collect().foreach(data => println(data.mkString(",")))


    sc.stop()
  }

}
