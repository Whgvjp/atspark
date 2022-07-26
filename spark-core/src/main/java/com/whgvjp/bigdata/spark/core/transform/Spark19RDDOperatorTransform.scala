package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
    // aggregateByKey最终的返回结果应该和初始值的类型保持一致
//    val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
//    aggRDD.collect.foreach(println)

    // combineByKey : 方法需要三个参数
    // 第一个参数表示： 将相同key的第一个数据进行结构的转换，实现操作
    // 第二个参数表示： 分区内的计算规则
    // 第三个参数表示： 分区间的计算规则
    val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
      v => (v,1),
      (t:(Int,Int), v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1:(Int,Int), t2:(Int,Int)) => {
        (t1._1 + t2._1, t1._2 + t2._2)
      }
    )

    val resRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) =>  num / cnt
    }
    resRDD.collect.foreach(println)

    sc.stop()
  }

}
