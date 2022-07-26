package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18RDDOperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
//    rdd.aggregateByKey(0)(_+_,_+_).collect().foreach(println)
    rdd.foldByKey(0)(_+_).collect.foreach(println)

    sc.stop()
  }

}
