package com.huntdreams.streaming.ch3

import java.io.{ObjectOutput, ObjectOutputStream}
import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.{SparkFlumeEvent, FlumeUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ScalaLoadDistributedEvents
 *
 * @author tyee.noprom@qq.com
 * @time 2/22/16 2:28 PM.
 */
object ScalaLoadDistributedEvents {

  def main(args: Array[String]) {
    println("Creating spark configuration")
    val conf = new SparkConf()
    conf.setAppName("Streaming data loading application")
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(2))

    //Create an Array of InetSocketaddress containing the  Host and the Port of the machines
    //where Flume Sink is delivering the Events
    //Basically it is the value of following properties defined in Flume Config: -
    //1. a1.sinks.spark.hostname
    //2. a1.sinks.spark.port
    //3. a2.sinks.spark1.hostname
    //4. a2.sinks.spark1.port
    var address = new Array[InetSocketAddress](2)
    address(0) = new InetSocketAddress("localhost", 4949)
    address(1) = new InetSocketAddress("localhost", 4950)

    //Create a Flume Polling Stream which will poll the Sink the get the events
    //from sinks every 2 seconds.
    //Last 2 parameters of this method are important as the
    //1.maxBatchSize = It is the maximum number of events to be pulled from the Spark sink
    //in a single RPC call.
    //2.parallelism  - The Number of concurrent requests this stream should send to the sink.
    //for more information refer to
    //https://spark.apache.org/docs/1.1.0/api/java/org/apache/spark/streaming/flume/FlumeUtils.html

    val flumeStream = FlumeUtils.createPollingStream(streamCtx,address,StorageLevel.MEMORY_AND_DISK_SER_2,1000,1)
    //Define Output Stream Connected to Console for printing the results
    val outputStream = new ObjectOutputStream(Console.out)
    //Invoking custom Print Method for writing Events to Console
    printValues(flumeStream, streamCtx, outputStream)

    //Most important statement which will initiate the Streaming Context
    streamCtx.start()
    //Wait till the execution is completed.
    streamCtx.awaitTermination()
  }

  def printValues(stream: DStream[SparkFlumeEvent], streamCtx: StreamingContext, outputStream: ObjectOutput): Unit = {
    stream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[SparkFlumeEvent]) => {
      val array = rdd.collect()
      println("------Start Printing Results------")
      println("Total size of Events = " + array.size)
      for (flumeEvent <- array) {
        //This is to get the AvroFlumeEvent from SparkFlumeEvent
        //for printing the Original Data
        val payLoad = flumeEvent // TODO check the bug
        println(new String(payLoad.toString))
      }
      println("---------Finished Printing Results----------")
    }
  }
}
