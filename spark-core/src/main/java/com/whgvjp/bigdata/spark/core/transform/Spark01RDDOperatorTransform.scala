package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDOperatorTransform {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    //TODO 算子 -map
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    // 转换函数
//    def mapFunction(num: Int):Int={
//      num*2
//    }

//    val rdd1 = rdd.map(mapFunction)
//    val rdd1 = rdd.map((num:Int)=>{num*2})
    val rdd1 = rdd.map(_*2)
    rdd1.collect().foreach(println)

    sc.stop()
  }

}
