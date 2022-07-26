package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(4,3,1,5,6,2),2)
    // TODO 算子 sortBy
    val sortRDD = rdd.sortBy(num => num)
    sortRDD.saveAsTextFile("output")


    sc.stop()
  }

}
