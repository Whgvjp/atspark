package com.whgvjp.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(scf)

    val orderActionRDD = sc.textFile("datas/2.txt")

    val flatRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(1)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    )
    val mapRDD: RDD[Array[(String, Int)]] = orderActionRDD.map(
      action => {
        val datas = action.split("_")
        val cid = datas(1)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    )
    mapRDD.map(_.mkString(",")).collect().foreach(println)
    val orderCountRDD = flatRDD.reduceByKey(_ + _)
    orderCountRDD.foreach(println)
//    orderCountRDD.collect().foreach(println)

    // 查看zip功能
//    println("********************************************")
//    val dataRDD1 = sc.makeRDD(List(1,2,3,4))
//    val dataRDD2 = sc.makeRDD(List(3,4,5,6))
//    val dataRDD = dataRDD1.zip(dataRDD2)
//    dataRDD.collect().foreach(println)

    sc.stop()
  }

}
