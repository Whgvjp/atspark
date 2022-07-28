package com.whgvjp.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkReq1HotCategoryTop10B {
  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(scf)

    // TODO: Top10热门品类
    // 1.读取数据
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // Q: 存在大量的shuffle操作，（reduceByKey）
    // reduceByKey 聚合算子，spark本身会提供优化，缓存

    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          // 点击场合
          List((datas(6), (1, 0, 0)))
        } else if (datas(8) != "null") {
          // 下单场合
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          // 支付场合
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )
    // 2.将数据转换结构
    // 点击场合： （品类ID，（1，0，0））
    // 下单场合： （品类ID，（0，1，0））
    // 支付场合： （品类ID，（0，0，1））
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey{
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }

    // 3. 将相同的品类的ID数据进行分组聚合
    //    （ 品类ID，（点击数量，下单数量，支付数量））

    // 4. 将统计结果根据数量进行降序排列取前十名
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)

    // 5.将结果采集到控制台打印出来
    resultRDD.foreach(println)

    sc.stop()
  }
}
