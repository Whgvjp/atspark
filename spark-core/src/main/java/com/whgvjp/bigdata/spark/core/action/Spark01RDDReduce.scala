package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDReduce {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.parallelize(Seq(1,2,3,4))
//    val i: Int = rdd.reduce((x,y)=>x+y)
//    println(i)

    // collect 采集，会将不同分区的数据按照分区顺序采集到driver端内存中，形成数组
//    val ints: Array[Int] = rdd.collect()
//    println(ints.mkString(","))

    val cnt: Long = rdd.count()
    println(cnt)

    val ft: Int = rdd.first()
    println(ft)

    val f3: Array[Int] = rdd.take(3)
    println(f3.mkString(","))

    val rdd1 = sc.parallelize(Seq(4,2,3,1))
    val ints: Array[Int] = rdd1.takeOrdered(3)(Ordering.Int.reverse)
    println(ints.mkString(","))

    sc.stop()
  }

}
