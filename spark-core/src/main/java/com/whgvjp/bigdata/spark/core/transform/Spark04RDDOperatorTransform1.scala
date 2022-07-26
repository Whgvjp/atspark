package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04RDDOperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(Seq("hello spark", "hello scala"))
    val res: RDD[String] = rdd.flatMap(
      s => {
        s.split(" ")
      }
    )
    res.collect()foreach(println)

    sc.stop()
  }

}
