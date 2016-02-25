package com.huntdreams.streaming.ch6

import java.net.InetSocketAddress

import com.huntdreams.streaming.ch4.ScalaLogAnalyzer
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
  * ScalaQueryingStreams
  *
  * @author tyee.noprom@qq.com
  * @time 2/25/16 9:39 PM.
  */
object ScalaQueryingStreams {

  def main(args: Array[String]) {
    // Creating Spark Configuration
    val conf = new SparkConf()
    conf.setAppName("Integrating Spark SQL")

    // Define Spark Context which we will use to initialize our SQLContext
    val sparkCtx = new SparkContext(conf)
    // Retrieving Streaming Context from Spark Context
    val streamCtx = new StreamingContext(sparkCtx, Seconds(10))
    // Defining Host for receiving data from Flume Sink
    var addresses = new Array[InetSocketAddress](1)
    addresses(0) = new InetSocketAddress("localhost", 4949)
    // Creating Flume Polling Stream
    val flumeStream = FlumeUtils.createPollingStream(streamCtx,
      addresses, StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)
    // Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    // Invoking map() operation to convert the log data into RDD of JSON Formatted String
    val newDstream = flumeStream.map { x =>
      transformLog.tansformLogDataIntoJSON(new String(x.event.getBody().array()))
    }
    // Defining Window Operation, So that we can execute
    // SQL Query on data received within a particular Window
    val wStream = newDstream.window(Seconds(40), Seconds(20))
    // Creating SQL DataFrame for each of the RDD's
    wStream.foreachRDD { rdd =>
      // Getting the SQL Context from Utility Method which
      // provides Singleton Instance of SQL Context
      val sqlCtx = getInstance(sparkCtx)
      // Converting JSON RDD into the SQL DataFrame by using jsonRDD() function
      val df = sqlCtx.jsonRDD(rdd)
      // creating and Registering the Temporary table for
      // the Converting DataFrame into table for further Querying
      df.registerTempTable("apacheLogData")
      // Print the Schema
      println("Here is the Schema of your Log Files............")
      df.printSchema()
      // Executing the Query to get the total count of
      // different HTTPResponse Code in the Data Frame
      val logDataFrame = sqlCtx.sql("select method, count(*) as total from apacheLogData group by method")
      // Finally printing the results on the Console
      println("Total Number of Requests.............. ")
      logDataFrame.show()
    }
    streamCtx.start()
    streamCtx.awaitTermination()
  }

  // Defining Singleton SQLContext variable
  @transient private var instance: SQLContext = null

  // Lazy initialization of SQL Context
  def getInstance(sparkContext: SparkContext): SQLContext =
    synchronized {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
}
