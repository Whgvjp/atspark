package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark19RDDOperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("b",3),("b",4),("b",5),("a",6)
    ),2)
    // aggregateByKey是功能最强大的，可以自定义初始值的处理方式，分区内、分区间的处理方式
    // combineByKey 是不给初始值，指定第一个值的处理方式，然后分区内、分区间的处理方式
    // foldByKey 是需要给定初始值的处理方式，然后分区内和分区间的逻辑是一样的
    // reduceByKey 是初始值就是第一个值不会参与计算，然后分区内和分区间逻辑一致

//    rdd.reduceByKey(_+_).collect.foreach(println)
//    rdd.aggregateByKey(0)(_+_,_+_).collect.foreach(println)
//    rdd.foldByKey(0)(_+_).collect.foreach(println)
    rdd.combineByKey(v=>v,(x:Int,y)=>x+y,(x:Int,y:Int)=>x+y).collect.foreach(println)

    sc.stop()
  }

}
