package com.whgvjp.bigdata.spark.framework.service

import com.whgvjp.bigdata.spark.framework.common.TService
import com.whgvjp.bigdata.spark.framework.dao.WordCountDao

class WordCountService extends TService{

  private val dao = new WordCountDao

  def dataAnalysis(): Array[(String, Int)] = {

    val lines = dao.readFile("datas/spark-core/wordCount")

    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))

    val wordGroup = wordToOne.groupBy(word => word._1)

    wordGroup.map {
      case (word, list) => {
        val tuple = list.reduce((a, b) => {
          (word, a._2 + b._2)
        })
        tuple
      }
    }.collect

  }
}
