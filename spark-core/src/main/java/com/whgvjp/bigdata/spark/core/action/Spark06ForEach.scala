package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark06ForEach {

    def main(args: Array[String]): Unit = {

      val sparConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
      val sc = new SparkContext(sparConf)

      val rdd = sc.makeRDD(Seq(1,2,3,4))

      // foreach 其实是driver端内存集合的循环遍历方法
      rdd.collect.foreach(println)
      println("****************************")
      // foreach 其实是executor端内存数据打印
      rdd.foreach(println)


      sc.stop()

    }
}
