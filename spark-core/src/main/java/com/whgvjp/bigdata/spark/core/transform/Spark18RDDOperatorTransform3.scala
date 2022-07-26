package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18RDDOperatorTransform3 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
    // aggregateByKey最终的返回结果应该和初始值的类型保持一致
//    val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
//    aggRDD.collect.foreach(println)

    // 获取相同key的数据的平均值 => (a,3), (b,4)
    val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
      (t, v) => {
        (t._1 + v, t._2 + 1)
      },
      (t1, t2) => {
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
