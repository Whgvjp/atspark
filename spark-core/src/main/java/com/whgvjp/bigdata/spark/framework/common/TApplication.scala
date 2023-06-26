package com.whgvjp.bigdata.spark.framework.common

import com.whgvjp.bigdata.spark.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

trait TApplication {

  def start(master: String = "local[*]", appName: String = "Application")(Op: => Unit) = {

    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

    val sc = new SparkContext(sparkConf)

    EnvUtil.put(sc)

    try {
      Op
    } catch {
      case ex => println(ex.getMessage)
    }

    sc.stop()
    EnvUtil.clear()
  }
}
