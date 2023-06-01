package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.sql.SparkSession

object CombineByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CombineByKeyExample").master("local").getOrCreate()

    // 创建一个包含键值对的RDD
    val rdd = spark.sparkContext.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("B", 4), ("C", 5)))

    // 使用combineByKey计算每个键的总和和计数
    //该函数接受三个参数：
    //
    //createCombiner：一个函数，用于将每个键的第一个值转换为累加器的初始值。它将一个值 V 转换为累加器的类型 C。该函数在每个分区中的每个键的第一个值上调用，
    // 用于初始化累加器。
    //mergeValue：一个函数，用于将每个分区中的值与累加器进行合并。它接受两个参数，累加器的类型 C 和值 V，并返回一个合并后的累加器。
    //mergeCombiners：一个函数，用于合并不同分区的累加器。它接受两个累加器 C，并返回一个合并后的累加器。
    val resultRDD = rdd.combineByKey(
      value => (value, 1),
      (acc: (Int, Int), value) => (acc._1 + value, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 打印结果
    resultRDD.collect().foreach { case (key, (sum, count)) => println(s"$key: sum=$sum, count=$count") }
  }

}
