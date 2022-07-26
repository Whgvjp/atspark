package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDAction {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.parallelize(Seq(1,2,3,4))
    rdd.collect()



    sc.stop()
  }

}
