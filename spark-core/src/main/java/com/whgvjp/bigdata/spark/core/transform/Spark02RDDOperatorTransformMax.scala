package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02RDDOperatorTransformMax {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    //TODO 算子 -map
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4,5,6), 2)
    val mpRDD: RDD[Int] = rdd.mapPartitions(
      iter => {
        List(iter.max).iterator
      }
    )
    // 最大值 [1,2,3],[4,5,6]
    mpRDD.collect().foreach(println)


    sc.stop()
  }

}
