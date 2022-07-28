package com.whgvjp.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01Acc {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))
    //    val res: Int = rdd.reduce(_ + _)
    //    println(res)

    // 获取系统累加器
    // spark默认就提供了简单数据聚合的累加器
    val sumAcc = sc.longAccumulator("sum")

    // 少加： 转换算子中调用累加器，如果没有行动算子，那么不会执行
    //    rdd.map(
    //      num => {
    //        sumAcc.add(num)
    //        num
    //      }
    //    )
    //    rdd.collect().foreach(println)
    // 多加： 下面出现了两次行动算子，然后转换算子会执行两次
    // 一般情况下累加器要放在行动算子中
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        sumAcc.add(num)
        num
      }
    )
    mapRDD.collect()
    mapRDD.collect()

    // 正常
    //    rdd.foreach(
    //      num => {
    //        sumAcc.add(num)
    //      }
    //    )
    println(sumAcc.value)

    //    错误方法
    //    var sum = 0
    //    rdd.foreach(
    //      num => {
    //        sum += num
    //      }
    //    )
    //    println("sum = " + sum)

    sc.stop()
  }

}
