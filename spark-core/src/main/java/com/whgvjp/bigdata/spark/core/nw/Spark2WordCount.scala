package com.whgvjp.bigdata.spark.core.nw

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark2WordCount {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount2")
    val context: SparkContext = new SparkContext(sconf)
    context.setLogLevel("WARN")

    val lineRDD: RDD[String] = context.textFile("datas")

    val stringRDD: RDD[String] = lineRDD.flatMap(_.split(" "))

    val wordToOne: RDD[(String, Int)] = stringRDD.map(
      word => (word, 1)
    )
    val wcre: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
//    val result: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
////    result.collect().foreach(println)
//    val wcre: RDD[(String, Int)] = result.map {
//      case (word, list) => {
//        list.reduce(
//          (t1, t2) => {
//            (t1._1, t1._2 + t2._2)
//          }
//        )
//      }
//    }
    wcre.foreach(println)

  }

}
