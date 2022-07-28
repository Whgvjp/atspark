package com.whgvjp.bigdata.spark.core.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

object SparkRDDPart {
  def main(args: Array[String]): Unit = {
    val sparConf = new SparkConf().setMaster("local").setAppName("Persist")
    val sc = new SparkContext(sparConf)

    val rdd = sc.makeRDD(List(
      ("nba", "xxxxx"),
      ("cba", "xxxxx"),
      ("wnba", "xxxxx"),
      ("nba", "xxxxx")
    ))
    //    new HashPartitioner()
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output")

    sc.stop()
  }

  /*
   * 自定义分区器
   * 1.继承Partitioner类
   * 2.重写方法
   */
  class MyPartitioner extends Partitioner {
    override def numPartitions: Int = 3

    // 返回数据的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba" => 0
        case "wnba" => 1
        case _ => 2
      }
    }
  }

}
