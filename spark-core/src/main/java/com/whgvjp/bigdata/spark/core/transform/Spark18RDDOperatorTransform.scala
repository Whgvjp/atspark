package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark18RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
    // aggregateByKey 存在函数柯里化，有两个参数列表
    // 第一个 参数列表，需要传递一个参数，表示为初始值
    //      主要用于当碰见第一个key的时候，和value进行分区内计算
    // 第二个参数列表需要传递两个参数
    //      第二个参数表示分区间计算规则
    //      第一个参数表示分区内计算规则
    rdd.aggregateByKey(0)(
//      (x,y) => x + y,
      (x,y) => math.max(x,y),
      (x,y) => x + y
    ).collect().foreach(println)

    sc.stop()
  }

}
