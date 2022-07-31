package com.whgvjp.bigdata.spark.core.framework.service

import com.whgvjp.bigdata.spark.core.framework.common.TService
import com.whgvjp.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService{

  private val wordCountDao = new WordCountDao()

  // 数据分析
  def dataAnalysis={
    // TODO 执行业务操作

    val lines: RDD[String] = wordCountDao.readFile("datas/word.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word=>(word,1))
    val wordToSum: RDD[(String, Int)] = wordToOne.reduceByKey(_+_)

    val array: Array[(String, Int)] = wordToSum.collect()
    array
  }

}
