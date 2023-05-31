package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.sql.SparkSession
object AggregateByKeyExample {
  def main(args: Array[String]): Unit = {

    // 创建SparkSession
    val spark = SparkSession.builder().appName("AggregateByKeyExample").master("local").getOrCreate()

    // 创建一个包含键值对的RDD
    val rdd = spark.sparkContext.parallelize(Seq(("A", 1), ("B", 2), ("A", 3), ("B", 4), ("C", 5)))

    // 使用aggregateByKey计算每个键的总和和计数
    val resultRDD = rdd.aggregateByKey((0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // 打印结果
    resultRDD.collect().foreach(println)

  }

}
