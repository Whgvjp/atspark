package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.sql.SparkSession

object CombineByKeyExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CombineByKeyExample").master("local").getOrCreate()

    // 创建一个包含键值对的RDD
    val rdd = spark.sparkContext.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("B", 4), ("C", 5)))

    // 使用combineByKey计算每个键的总和和计数
    val resultRDD = rdd.combineByKey(
      value => (value, 1),
      (acc: (Int, Int), value) => (acc._1 + value, acc._2 + 1),
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 打印结果
    resultRDD.collect().foreach { case (key, (sum, count)) => println(s"$key: sum=$sum, count=$count") }
  }

}
