package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark05Save {

    def main(args: Array[String]): Unit = {

      val sparConf = new SparkConf().setMaster("local").setAppName("WordCount")
      val sc = new SparkContext(sparConf)

      val rdd = sc.makeRDD(Seq(
        ("a",1),("a",2),("a",3)
      ))

      rdd.saveAsTextFile("output")
      rdd.saveAsObjectFile("output1")
      // saveAsSequenceFile 要求数据格式必须为K-V类型
      rdd.saveAsSequenceFile("output2")


      sc.stop()

    }
}
