package com.whgvjp.bigdata.spark.core.readbook

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01Rdd {

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("createRdd")
    val sparkContext: SparkContext = new SparkContext(sparkConf)

    val rdd1: RDD[Int] = sparkContext.parallelize(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sparkContext.makeRDD(List(5,6,7,8))
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)

    sparkContext.stop()

  }

}
