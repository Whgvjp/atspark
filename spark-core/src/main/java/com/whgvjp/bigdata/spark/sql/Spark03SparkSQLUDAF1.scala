//package com.atguigu.bigdata.spark.sql
//import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
//import org.apache.spark.sql.expressions.Aggregator
//
//case class Score(name: String, score: Double)
//class AverageAggregator extends Aggregator[Score, (Double, Int), Double] {
//  // 初始化缓冲区，设置初始值
//  def zero: (Double, Int) = (0.0, 0)
//  // 在分区内对值进行累加
//  def reduce(buffer: (Double, Int), score: Score): (Double, Int) = {
//    (buffer._1 + score.score, buffer._2 + 1)
//  }
//  // 在分区之间合并结果
//  def merge(buffer1: (Double, Int), buffer2: (Double, Int)): (Double, Int) = {
//    (buffer1._1 + buffer2._1, buffer1._2 + buffer2._2)
//  }
//  // 计算最终结果
//  def finish(buffer: (Double, Int)): Double = {
//    buffer._1 / buffer._2
//  }
//  // 定义缓冲区的编码
//  def bufferEncoder: Encoder[(Double, Int)] = Encoders.tuple(Encoders.scalaDouble, Encoders.scalaInt)
//  // 定义输出结果的编码
//  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
//}
//
//object AverageAggregator {
//  def main(args: Array[String]): Unit = {
//    // 创建 SparkSession
//    val spark = SparkSession.builder().appName("UDAF-TEST").master("local[*]").getOrCreate()
//    import spark.implicits._
//
//    // 创建包含学生成绩的 DataFrame
//    val data = Seq(
//      Score("Alice", 90.0),
//      Score("Bob", 85.0),
//      Score("Charlie", 95.0),
//      Score("Alice", 92.0),
//      Score("Bob", 88.0),
//      Score("Charlie", 96.0)
//    ).toDS()
//
//    // 创建聚合器实例
//    val aggregator = new AverageAggregator
//
//    // 应用聚合器计算学生的平均分
//    val result = data.select(aggregator.toColumn.alias("average_score"))
//
//    // 显示结果
//    result.show()
//    // 停止SparkSession
//    spark.stop()
//  }
//
//}
//
