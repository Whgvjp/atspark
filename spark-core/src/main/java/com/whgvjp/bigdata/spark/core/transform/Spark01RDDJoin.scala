package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDJoin {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3),("a",5)))
    val rdd2 = sc.parallelize(Seq(("a", 4), ("b", "whgvjp"), ("c", 6),("a","ks")))

    // 两个不同数据源的数据，相同key的value会连接在一起，形成元组
    // 如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
    // 如果两个数据源中key有多个相同的，会依次匹配，会出现笛卡尔乘积，数据量几何增长，会有内存风险性能降低
    val joinRDD = rdd1.join(rdd2)
    joinRDD.collect.foreach(println)


    sc.stop()
  }

}
