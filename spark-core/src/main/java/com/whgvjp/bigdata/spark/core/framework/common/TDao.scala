package com.whgvjp.bigdata.spark.core.framework.common

import com.whgvjp.bigdata.spark.core.framework.util.EnvUtil

trait TDao {
  def readFile(path:String) ={
    // 1. 读取文件，获取一行一行的数据
    EnvUtil.take().textFile(path)
  }
}
