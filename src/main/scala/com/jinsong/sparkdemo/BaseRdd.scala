package com.jinsong.sparkdemo
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
object BaseRdd {
  def main(args: Array[String]): Unit = {

    //创建sc
    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("BaseRdd")
    conf.set("log4j.rootCategory","ERROR, console")
    val sc = new SparkContext(conf)

    val lines = List(1,2,3,3)
    val lines2 = List("a b","c d","e","f")

    val rdd = sc.parallelize(lines)
    val rdd2 = sc.parallelize(lines2)

    //转化操作
    val rddmap = rdd.map(a=>a+1)
    val rddmapc = rddmap.collect()

    val rdd2flatmap = rdd2.flatMap( a=>a.split(" "))

    val rddfilter = rdd.filter(a=>a>2)

    rddmapc.foreach(println)
    rddfilter.foreach(println)
    rdd2flatmap.foreach(a=>a.foreach(println))


  }
}
