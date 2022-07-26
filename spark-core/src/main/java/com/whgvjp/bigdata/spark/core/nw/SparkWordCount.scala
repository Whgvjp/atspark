package com.whgvjp.bigdata.spark.core.nw

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val context = new SparkContext(conf)
    context.setLogLevel("WARN")

    val lines: RDD[String] = context.textFile("datas")
    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordToCount: RDD[(String, Int)] = wordGroup.map{
      case (word, list) => {
        (word, list.size)
      }
    }
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    
    

  }

}
