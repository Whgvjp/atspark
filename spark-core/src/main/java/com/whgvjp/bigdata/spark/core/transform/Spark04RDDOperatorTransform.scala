package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd: RDD[Seq[Int]] = sc.makeRDD(Seq(Seq(1, 2), Seq(3, 4)))
    //TODO 算子 -map
    val flatRDD: RDD[Int] = rdd.flatMap(
      list => {
        list
      }
    )
    flatRDD.collect().foreach(println)

    sc.stop()
  }

}
