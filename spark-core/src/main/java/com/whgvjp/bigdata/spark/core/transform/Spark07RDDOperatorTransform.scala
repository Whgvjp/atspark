package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark07RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(Seq(1,2,3,4))
    val filterRDD = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)


    sc.stop()
  }

}
