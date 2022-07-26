package com.whgvjp.bigdata.spark.core.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark07ForEach1 {

    def main(args: Array[String]): Unit = {

      val sparConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
      val sc = new SparkContext(sparConf)

      val rdd = sc.makeRDD(Seq[Int]())

      val user = new User()

      // RDD 算子中传递的函数是会包含闭包操作，那么就会进行检测功能； foreach 后面闭包
      // 闭包检测 所以这里即使rdd没有值，也会被闭包检测到 user没有序列化
      rdd.foreach(
        num => {
          println("age = " + (user.age + num))
        }
      )

      sc.stop()

    }
//    class User extends Serializable {
  // 样例类在编译时，会自动混入序列化特质（实现可序列化接口）
//    case class User(){
    class User{
      var age : Int = 30
    }
}
