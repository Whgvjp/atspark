package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDWordCount {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)
//    wordCount1(sc)
//    wordCount2(sc)
//    wordCount3(sc)
//    wordCount4(sc)
//    wordCount5(sc)
//    wordCount6(sc)
//    wordCount7(sc)
    wordCount8(sc)


    sc.stop()
  }

  def wordCount1(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordCount2(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }
  def wordCount3(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = wordOne.reduceByKey(_+_)
  }
  def wordCount4(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_,_+_)
  }
  def wordCount5(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
  }

  def wordCount6(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Int)] = wordOne.combineByKey(
      v=>v,
      (x:Int,y)=> x+y,
      (x:Int,y:Int)=>x+y
    )
  }

  def wordCount7(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val stringToLong: collection.Map[String, Long] = wordOne.countByKey()
  }
  def wordCount8(sc:SparkContext): Unit = {
    val rdd: RDD[String] = sc.makeRDD(Seq("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordcount: collection.Map[String, Long] = words.countByValue()

  }

}
