package com.huntdreams.streaming.ch2

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * StreamingExample
 *
 * @author tyee.noprom@qq.com
 * @time 2/21/16 12:45 PM.
 */
object StreamingExample {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("My first spark streaming application")
    val streamCtx = new StreamingContext(conf, Seconds(2))
    //Define the the type of Stream. Here we are using TCP Socket as text stream,
    //It will keep watching for the incoming data from a specific machine (localhost) and port (9087)
    //Once the data is retrieved it will be saved in the memory and in case memory
    //is not sufficient, then it will store it on the Disk
    //It will further read the Data and convert it into DStream

    val lines = streamCtx.socketStream("localhost", 9087)
  }
}
