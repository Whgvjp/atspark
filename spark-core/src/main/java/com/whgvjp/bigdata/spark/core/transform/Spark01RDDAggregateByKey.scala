package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDAggregateByKey {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd: RDD[(String, Int)] = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),2)
    val aggRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
      (x, y) => math.max(x, y),
      (x, y) => x + y
    )
    aggRDD.collect.foreach(println)

    sc.stop()
  }

}
