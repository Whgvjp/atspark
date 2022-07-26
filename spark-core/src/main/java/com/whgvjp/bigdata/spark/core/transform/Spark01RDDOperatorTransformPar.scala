package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDOperatorTransformPar {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    //TODO 算子 -map
    val rdd = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    rdd.saveAsTextFile("output1")

    val mapRDD = rdd.map(_*2)
    // 证明map后分区数不变，且分区也不变
    mapRDD.saveAsTextFile("output")

    sc.stop()
  }

}
