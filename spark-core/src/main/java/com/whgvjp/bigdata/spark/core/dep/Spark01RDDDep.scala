package com.whgvjp.bigdata.spark.core.dep

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark01RDDDep {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sconf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.toDebugString)
    println("******************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("******************************")
    val word2one: RDD[(String, Int)] = words.map(word => (word, 1))
    println(word2one.toDebugString)
    println("******************************")
    val result: RDD[(String, Int)] = word2one.reduceByKey(_ + _)
    println(result.toDebugString)
    println("******************************")
    val arrs: Array[(String, Int)] = result.collect()
    arrs.foreach(println)
    sc.stop()
  }

}
