package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDOperatorTransformTest {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    //TODO 算子 -map
    val rdd = sc.textFile("datas/apache.log")
    val mapRDD = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        datas(6)
      }
    )
    mapRDD.collect().foreach(println)



    sc.stop()
  }

}
