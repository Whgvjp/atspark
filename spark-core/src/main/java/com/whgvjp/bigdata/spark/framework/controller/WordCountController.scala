package com.whgvjp.bigdata.spark.framework.controller

import com.whgvjp.bigdata.spark.framework.common.TController
import com.whgvjp.bigdata.spark.framework.service.WordCountService

class WordCountController extends TController{

  private val service = new WordCountService

  // 调度
  def dispatch (): Unit = {

    val wordCount: Array[(String, Int)] = service.dataAnalysis()

    wordCount.foreach(println)
  }
}
