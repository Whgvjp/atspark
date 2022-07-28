package com.whgvjp.bigdata.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark02AccWordCount {
  def main(args: Array[String]): Unit = {
    val sconf: SparkConf = new SparkConf().setMaster("local").setAppName("rddTransform")
    val sc: SparkContext = new SparkContext(sconf)

    val rdd = sc.parallelize(Seq("hello","saprk","hello","scala"))

//    rdd.map((_,1)).reduceByKey(_+_)
    // 累加器 : WordCount
    // 创建累加器对象
    val wcAcc = new MyAccumulator
    // 向spark注册
    sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(
      word => {
        wcAcc.add(word)
      }
    )

    println(wcAcc.value)

    sc.stop()
  }

  /**
   * 自定义累加器： WordCount
   * 1. 继承AcculatorV2, 定义泛型
   *    IN: 累加器输入的数据类型 String
   *    OUT: 累加器返回的数据类型 mutable.Map[String,Long]
   * 2. 重写方法
   */
  class MyAccumulator extends AccumulatorV2[String,mutable.Map[String,Long]]{
    private var wcMap = mutable.Map[String,Long]()
    // 判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }

    override def copy(): AccumulatorV2[String,mutable.Map[String,Long]] = {
      new MyAccumulator()
    }

    override def reset(): Unit = {
      wcMap.clear()
    }

    // 获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCnt:Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word, newCnt)
    }

    // Driver合并多个累加器
    override def merge(other: AccumulatorV2[String,mutable.Map[String,Long]]): Unit = {
      val map1 = this.wcMap
      val map2 = other.value
      map2.foreach{
        case (word, count) => {
          val newCount = map1.getOrElse(word, 0L) + count
          map1.update(word, newCount)
        }
      }
    }

    // 累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }

}
