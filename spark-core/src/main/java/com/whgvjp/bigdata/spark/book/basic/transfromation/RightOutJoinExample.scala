package com.whgvjp.bigdata.spark.book.basic.transfromation

import org.apache.spark.sql.SparkSession

object RightOutJoinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("right out join example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD1 = sc.parallelize(Array[(Int,Char)](
      (3,'c'),(3,'f'),(1,'a'),(4,'d'),(1,'h'),(2,'b'),(5,'e'),(2,'g')
    ),3)
    val inputRDD2 = sc.parallelize(Array[(Int,Char)](
      (1,'A'),(2,'B'),(3,'C'),(4,'D'),(6,'E')
    ),2)

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.rightOuterJoin(inputRDD2,2)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
