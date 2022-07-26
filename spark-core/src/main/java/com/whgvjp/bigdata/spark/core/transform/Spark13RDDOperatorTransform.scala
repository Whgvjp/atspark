package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark13RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd1 = sc.makeRDD(Seq(1,2,2,3,3,4))
    val rdd2 = sc.makeRDD(Seq(3,4,5,6,7,7))
    val rdd7 = sc.makeRDD(Seq("3","4","5","6"))

    // 交集、并集、差集 类型不相同是不能做运算的，但是zip是可以的

    // 交集 [3,4] 交集结果是不会有重复值的
    val rdd3 = rdd1.intersection(rdd2)
    println(rdd3.collect().mkString(","))
    // 并集 [1,2,2,3,3,4,3,4,5,6,7,7]
    val rdd4 = rdd1.union(rdd2)
    println(rdd4.collect().mkString(","))
    // 差集 [1,2,2] 但是差集是会有重复的
    val rdd5 = rdd1.subtract(rdd2)
    println(rdd5.collect().mkString(","))
    // 拉链 [(1,3),(2,4),(2,5),(3,6),(3,7),(4,7)]
    val rdd6 = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


    sc.stop()
  }

}
