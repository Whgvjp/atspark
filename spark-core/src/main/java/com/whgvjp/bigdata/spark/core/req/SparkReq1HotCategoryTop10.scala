package com.whgvjp.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkReq1HotCategoryTop10 {
  def main(args: Array[String]): Unit = {
    val scf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(scf)

    // TODO: Top10热门品类
    // 1.读取数据
    val actionRDD = sc.textFile("datas/user_visit_action.txt")

    // 2.统计品类点击数量：（品类ID，点击数量）
    val clickActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(6) != "-1"
      }
    )
    val clickCountRDD = clickActionRDD.map(
      action => {
        val datas = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_ + _)

    // 3.统计品类下单数量：（品类ID，下单数量）
    val orderActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(8) != "null"
      }
    )

    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(8)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 4.统计品类支付数量：（品类ID，支付数量）
    val payActionRDD = actionRDD.filter(
      action => {
        val datas = action.split("_")
        datas(10) != "null"
      }
    )

    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas = action.split("_")
        val cid = datas(10)
        val cids = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    // 5.将品类进行排序，并且取前10名
    //   点击数量排序，下单数量排序，支付数量排序
    //   元组排序： 先比较第一个，再比较第二个，再比较第三个，以此类推
    //   (品类ID, (点击数量,下单数量,支付数量))
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] =
        clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
      case (clickIter, orderIter, payIter) => {
        var clickCnt = 0
        val iter1 = clickIter.iterator
        if (iter1.hasNext) {
          clickCnt = iter1.next()
        }
        var orderCnt = 0
        val iter2 = orderIter.iterator
        if (iter2.hasNext) {
          orderCnt = iter2.next()
        }
        var payCnt = 0
        val iter3 = payIter.iterator
        if (iter3.hasNext) {
          payCnt = iter3.next()
        }

        (clickCnt, orderCnt, payCnt)
      }
    }

    // 6.将结果采集到控制台打印出来
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    resultRDD.foreach(println)

    sc.stop()
  }
}
