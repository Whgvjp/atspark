package com.whgvjp.bigdata.spark.core.presist

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDDPersist {
  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(scf)

    val list = List("Hello Spark", "Hello Scala")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))
    val mapRDD = flatRDD.map(word=>{
      println("$$$$$$$$$$$$$$$$$$$$$$")
      (word,1)
    })
    // cache 默认持久化到内存中
//    mapRDD.cache()
    mapRDD.persist(StorageLevel.DISK_ONLY)
    val resultRDD = mapRDD.reduceByKey(_ + _)

    resultRDD.collect().foreach(println)

    println("*****************************")

    val groupRDD = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)


    sc.stop()
  }

}
