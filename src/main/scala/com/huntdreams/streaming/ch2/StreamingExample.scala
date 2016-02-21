package com.huntdreams.streaming.ch2

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
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
    //Apply the Split() function to all elements of DStream
    //which will further generate multiple new records from each record in Source Stream
    //And then use flatmap to consolidate all records and create a new DStream.
    val words = lines.flatMap(x => x.split(" "))
    //Now, we will count these words by applying a using map()
    //map() helps in applying a given function to each element in an RDD.
    val pairs = words.map(word => (word, 1))
    //Further we will aggregate the value of each key by using/ applying the given function.
    val wordCounts = pairs.reduceByKey(_ + _)

    //Lastly we will print all Values
    //wordCounts.print(20)

    printValues(wordCounts,streamCtx)
    //Most important statement which will initiate the Streaming Context
    streamCtx.start();
    //Wait till the execution is completed.
    streamCtx.awaitTermination();
  }

  def printValues(stream: DStream[(String, Int)], streamCtx: StreamingContext): Unit = {
    stream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[(String, Int)]) => {
      val array = rdd.collect()
      println("---------Start Printing Results----------")
      for (res <- array) {
        println(res)
      }
      println("---------Finished Printing Results----------")
    }
  }
}
