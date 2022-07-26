package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDPartitions {
  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setMaster("local[*]").setAppName("Transform1")
    val sc = new SparkContext(scf)

    val rdd = sc.makeRDD(List(1,2,3,4,1,2,3,4))
    val partitions = rdd.getNumPartitions
    println(partitions)
//    List(1,1,2,2).distinct.foreach(println)

    sc.stop()
  }

}
