package com.whgvjp.bigdata.spark.framework.application

import com.whgvjp.bigdata.spark.framework.common.TApplication
import com.whgvjp.bigdata.spark.framework.controller.WordCountController

object WordCountApplication extends App with TApplication{

  start() {
    val controller = new WordCountController

    controller.dispatch()
  }

}
