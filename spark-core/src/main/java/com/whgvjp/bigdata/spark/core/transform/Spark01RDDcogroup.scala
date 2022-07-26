package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDcogroup {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd1 = sc.parallelize(Seq(("a", 1),("a",9), ("b", 2), ("c", 3),("d",5)))
    val rdd2 = sc.parallelize(Seq(("a", 4), ("b", "whgvjp"), ("c", 6),("e",8)))

    // cogroup : connect + group
    rdd1.cogroup(rdd2).collect.foreach(println)


    sc.stop()
  }

}
