package com.whgvjp.bigdata.spark.framework.common

import com.whgvjp.bigdata.spark.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

trait TDao {

  def readFile(path: String): RDD[String] = {

    EnvUtil.take.textFile(path)
  }

}
