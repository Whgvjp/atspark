package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.sql.SparkSession
object AggregateByKeyExample {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession.builder().appName("AggregateByKeyExample").master("local").getOrCreate()

    // 创建一个包含键值对的RDD
    val rdd = spark.sparkContext.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("B", 4), ("C", 5)))

    // 使用aggregateByKey计算每个键的总和和计数，两组参数
    //在(acc, value) => (acc._1 + value, acc._2 + 1)这个函数中，acc表示累加器，而value表示RDD中的元素值。每次对一个键值对进行聚合操作时，
    // acc表示当前键的累加器的值，而value表示当前键对应的元素值。
    //
    //在示例中，累加器acc是一个元组，包含两个值(acc._1, acc._2)。其中，acc._1表示当前键已经累加的总和，acc._2表示当前键已经出现的次数。
    // 通过(acc._1 + value, acc._2 + 1)这个函数，每次聚合操作会将当前键的值累加到acc._1上，并将acc._2加1，实现对每个键的总和和计数的聚合计算。
    val resultRDD = rdd.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 打印结果
    resultRDD.collect().foreach(println)

  }

}
