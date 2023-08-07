package com.whgvjp.bigdata.spark.sql

import scala.collection.mutable
object TestMap {
  def main(args: Array[String]): Unit = {
    val map1: mutable.Map[String, Long] = mutable.Map(
      "key1" -> 10,
      "key2" -> 20,
      "key3" -> 30
    )

    val map2: mutable.Map[String, Long] = mutable.Map(
      "key4" -> 10,
      "key2" -> 20,
      "key3" -> 30
    )

    map2.foreach {
      case (city, cnt) => {
        val newCount = map1.getOrElse(city, 0L) + cnt
        map1.update(city, newCount)
      }
    }

    map1.foreach(println)
  }

}
