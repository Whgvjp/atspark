package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDTest {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val dataRDD: RDD[String] = sc.textFile("datas/agent.log")
    val mapRDD: RDD[((String, String), Int)] = dataRDD.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1),datas(4)),1)
      }
    )

    // (（省份，广告），sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)
    // (（省份），（广告，sum）)
    val newMapRDD = reduceRDD.map{
      case ((prv,ad), sum) => {
        (prv, (ad,sum))
      }
    }
    // (省份，【(广告A,sumA),(广告B,sumB)】)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    // 降序取前三名
    val resultRDD: RDD[(String, Seq[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toSeq.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )
    resultRDD.collect().foreach(println)



    sc.stop()
  }

}
