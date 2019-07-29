package com.test.kudu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object TestPanda {
  def main(args: Array[String]) {
    System.setProperty("HADOOP_USER_NAME", "hdfs");

    val sparkConf = new SparkConf()
    sparkConf.setAppName(s"TestPanda")
      .set("spark.hadoop.fs.defaultFS", "hdfs://172.0.3.165:8020")
    //      .setMaster("spark://1:7077")
    val sc = new SparkContext(sparkConf);
    sc.setLogLevel("WARN")

    val data = sc.textFile("/test/*", 1).flatMap(x => {
      val s = x.split("\\|")
      s
    }).collect().foreach(x => {
      println("x")
    })

    sc.stop()

  }
}