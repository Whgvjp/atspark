package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDCountByKey {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

//    val rdd = sc.parallelize(Seq(1,2,3,2,4))
//
//    val i: collection.Map[Int, Long] = rdd.countByValue()
//    println(i)

    val rdd2 = sc.parallelize(Seq(("a",1),("a",2),("a",3),("b",4)))
    val stringToLong: collection.Map[String, Long] = rdd2.countByKey()
    println(stringToLong)


    sc.stop()
  }

}
