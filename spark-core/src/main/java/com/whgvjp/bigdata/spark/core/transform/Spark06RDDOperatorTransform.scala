package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4), 2)
    // groupBy会将数据源中的每一个数据进行分组判断，根据返回的分组key进行分组
    // 相同的key值的数据会放置在一个组中
    def groupFunction(num: Int)={
      num % 2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
