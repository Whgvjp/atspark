package com.whgvjp.bigdata.spark.core.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01RDDDep2 {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sconf)
    sc.setLogLevel("WARN")

    val lines: RDD[String] = sc.textFile("datas/word.txt")
    println(lines.dependencies)
    println("******************************")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("******************************")
    val word2one: RDD[(String, Int)] = words.map(word => (word, 1))
    println(word2one.dependencies)
    println("******************************")
    val result: RDD[(String, Int)] = word2one.reduceByKey(_ + _)
    println(result.dependencies)
    println("******************************")
    val arrs: Array[(String, Int)] = result.collect()
    arrs.foreach(println)
    sc.stop()
  }

}
