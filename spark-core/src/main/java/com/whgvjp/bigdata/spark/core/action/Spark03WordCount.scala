package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03WordCount {

    def main(args: Array[String]): Unit = {

      val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(sparConf)
      wordcount10(sc)

      sc.stop()

    }

    // groupBy
    def wordcount1(sc : SparkContext): Unit = {

      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val group: RDD[(String, Iterable[String])] = words.groupBy(word=>word)
      val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // groupByKey
    def wordcount2(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
      val wordCount: RDD[(String, Int)] = group.mapValues(iter=>iter.size)
    }

    // reduceByKey
    def wordcount3(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_+_)
    }

    // aggregateByKey
    def wordcount4(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_+_, _+_)
    }

    // foldByKey
    def wordcount5(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_+_)
    }

    // combineByKey
    def wordcount6(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
        v=>v,
        (x:Int, y) => x + y,
        (x:Int, y:Int) => x + y
      )
    }

    // countByKey
    def wordcount7(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordOne = words.map((_,1))
      val wordCount: collection.Map[String, Long] = wordOne.countByKey()
    }

    // countByValue
    def wordcount8(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words = rdd.flatMap(_.split(" "))
      val wordCount: collection.Map[String, Long] = words.countByValue()
    }

    // reduce
    def wordcount9(sc : SparkContext): Unit = {
      val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
      val words: RDD[String] = rdd.flatMap(_.split(" "))

      // 【（word, count）,(word, count)】
      // word => Map[(word,1)]
      val mapWord: RDD[mutable.Map[String, Long]] = words.map(
        word => {
          mutable.Map[String, Long]((word,1))
        }
      )

      val wordCount: mutable.Map[String, Long] = mapWord.reduce(
        (map1, map2) => {
          map2.foreach{
            case (word, count) => {
              val newCount = map1.getOrElse(word, 0L) + count
              map1.update(word, newCount)
            }
          }
          map1
        }
      )
      println(mapWord)
    }
  // aggregate
  def wordcount10(sc : SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words: RDD[String] = rdd.flatMap(_.split(" "))

    // 【（word, count）,(word, count)】
    // word => Map[(word,1)]
    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word,1))
      }
    )

    val wordcount = mapWord.aggregate(mutable.Map[String, Long]((mapWord.first().keySet.head,0)))(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }, (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )

    println(wordcount)
  }
  // fold
  def wordcount11(sc: SparkContext): Unit ={
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    // word => Map[(word,1)]
    val mapRDD: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        // mutable 操作起来比较方便
        mutable.Map[String, Long]((word, 1))
      }
    )

    // Map 和 Map 聚合
    val wordcount = mapRDD.fold(mutable.Map[String, Long]((mapRDD.first().keySet.head,0)))(
      (map1, map2) => {
        map2.foreach {
          case (word, count) => {
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
          }
        }
        map1
      }
    )

    println(wordcount)
  }
}
