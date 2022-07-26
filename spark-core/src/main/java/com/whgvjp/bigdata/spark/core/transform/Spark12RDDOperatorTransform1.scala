package com.whgvjp.bigdata.spark.core.transform

import org.apache.spark.{SparkConf, SparkContext}

object Spark12RDDOperatorTransform1 {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[*]").setAppName("Transform")
    val sc = new SparkContext(sconf)

    val rdd = sc.makeRDD(List(("1",4),("11",2),("2",3)),2)
    // TODO 算子 sortBy
    // sortBy方法可以根据指定的规则对数据源中的数据进行排序，默认为升序，第二个参数可以设置为false即降序
    // sortBy默认情况下不会改变分区但是会有shuffle
    val sortRDD = rdd.sortBy(e => e._1.toInt,false)
    sortRDD.collect().foreach(println)


    sc.stop()
  }

}
