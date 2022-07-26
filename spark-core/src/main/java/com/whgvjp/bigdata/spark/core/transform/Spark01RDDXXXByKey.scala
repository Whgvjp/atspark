package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDXXXByKey {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("b", 4), ("b", 5), ("a", 6)),2)
    rdd.reduceByKey(_+_).collect.foreach(println)
    rdd.aggregateByKey(0)(_+_,_+_).collect.foreach(println)
    rdd.foldByKey(0)(_ + _).collect.foreach(println)
    rdd.combineByKey((u=>u),(u:Int,v)=>u+v,(u:Int,v:Int)=>u+v).collect.foreach(println)

    sc.stop()
  }

}
