package com.huntdreams.streaming.ch4

import java.net.InetSocketAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * ScalaTransformLogEvents
 *
 * @author tyee.noprom@qq.com
 * @time 2/23/16 11:37 AM.
 */
object ScalaTransformLogEvents {

  def main(args: Array[String]) {
    /** Start Common piece of code for all kinds of Transform operations */
    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("Apache Log Transformer")
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(10))

    var addresses = new Array[InetSocketAddress](1)
    addresses(0) = InetSocketAddress("localhost", 4949)
    val flumeStream =
      FlumeUtils.createPollingStream(streamCtx, addresses, StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)

    //Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    //Invoking Flatmap operation to flatening the results and convert them into Key/Value pairs
    val newDstream = flumeStream.flatMap { x =>
      transformLog.tansfromLogData(new String(x.event.getBody().array()))
    }

    /** End Common piece of code for all kinds of Transform operations */
    /** Start - Transformation Functions */
    executeTransformations(newDstream, streamCtx)

    /** End - Transformation Functions */
    streamCtx.start()
    streamCtx.awaitTermination()
  }

  /**
   * Define and execute all Transformations to the log data
   */
  def executeTransformations(dStream: DStream[(String, String)], streamCtx: StreamingContext) {}

  /**
   * Print the values
   *
   * @param stream stream
   * @param streamCtx streamCtx
   */
  def printValues(stream: DStream[(String, String)], streamCtx: StreamingContext): Unit = {
    //Implementing ForEach function for printing all the data in provided DStream
    stream.foreachRDD(foreachFunc)
    def foreachFunc = (rdd: RDD[(String, String)]) => {
      //collect() method fetches the data from all partitions and "collects" at driver node.
      //So in case data is too huge than driver may crash.
      //In production environments we persist this RDD data into HDFS or use the rdd.take(n) method.
      val array = rdd.collect()
      println("---------Start Printing Results----------")
      for (dataMap <- array.array) {
        print(dataMap._1, "-----", dataMap._2)
      }
      println("---------Finished Printing Results----------")
    }
  }
}