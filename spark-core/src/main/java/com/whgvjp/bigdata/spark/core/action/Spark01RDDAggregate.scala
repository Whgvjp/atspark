package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDAggregate {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.parallelize(Seq(1,2,3,4),2)

    // 13 + 17 = 30?
    // 结果是40 ！
    // aggregateByKey: 初始值只会参与分区内计算
    // aggregate: 初始值会参与分区内计算，并且会参与分区间计算
//    val res: Int = rdd.aggregate(10)(_ + _, _ + _)
    val res: Int = rdd.fold(10)(_ + _)
    println(res)

    sc.stop()
  }

}
