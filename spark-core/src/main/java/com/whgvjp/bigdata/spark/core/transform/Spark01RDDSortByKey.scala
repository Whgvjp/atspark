package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDSortByKey {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("z", 2), ("g", 3), ("b", 4), ("j", 5), ("w", 6)),2)
    val sortedRDD: RDD[(String, Int)] = rdd.sortByKey(true)
    sortedRDD.collect.foreach(println)

    sc.stop()
  }

}
