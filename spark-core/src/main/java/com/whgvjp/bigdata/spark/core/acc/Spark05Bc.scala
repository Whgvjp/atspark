package com.whgvjp.bigdata.spark.core.acc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05Bc {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd1 = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))

    //("a", 1),  ("b", 2),  ("c", 3)
    //(a,(1,4)), (b,(2,5)), (c,(3,6))
    val map = mutable.Map(("a", 4), ("b", 5), ("c", 36))
    rdd1.map{
      case (w, c) => {
        val l = map.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)


//    val rdd2 = sc.makeRDD(List(
//      ("a", 4), ("b", 5), ("c", 6)
//    ))
//
//    val joinRDD = rdd1.join(rdd2)
//    joinRDD.collect().foreach(println)

    sc.stop()
  }

}
