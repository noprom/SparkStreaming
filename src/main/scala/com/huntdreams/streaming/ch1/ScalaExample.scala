package com.huntdreams.streaming.ch1

import org.apache.spark.{SparkConf, SparkContext}

/**
 * ScalaExample
 *
 * @author tyee.noprom@qq.com
 * @time 2/21/16 12:24 PM.
 */
object ScalaExample {

  def main(args: Array[String]) {
    println("Creating Spark configuration")
    val conf = new SparkConf()
    conf.setAppName("My First Spark Scala App")
//    conf.setMaster("local")

    val ctx = new SparkContext(conf)
    val file = "/Users/noprom/Documents/Dev/Spark/Pro/SparkStreaming/pom.xml"
    val logData = ctx.textFile(file, 2)

    // Count the lines
    val numLines = logData.filter(line => true).count()
    println("Number of lines of the dataset " + numLines)
    Thread.sleep(100000)
  }
}
