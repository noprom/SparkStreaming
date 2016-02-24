package com.huntdreams.streaming.ch5

import java.net.InetSocketAddress

import com.datastax.spark.connector.SomeColumns
import com.huntdreams.streaming.ch4.ScalaLogAnalyzer
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * ScalaPersistInCassandra
  *
  * @author tyee.noprom@qq.com
  * @time 2/24/16 8:35 PM.
  */
object ScalaPersistInCassandra {

  def main(args: Array[String]) {
    /** Start Common piece of code for all kinds of Output Operations */
    println("Creating Spark Configuration")
    val conf = new SparkConf()
    conf.setAppName("Apache Log Persister in Cassandra")
    // Cassandra Host Name
    println("Setting Cassandra Host Name for getting Connection")
    conf.set("spark.cassandra.connection.host", "localhost")
    println("Retreiving Streaming Context from Spark Conf")
    val streamCtx = new StreamingContext(conf, Seconds(10))
    var addresses = new Array[InetSocketAddress](1);
    addresses(0) = new InetSocketAddress("localhost", 4949)
    val flumeStream =
      FlumeUtils.createPollingStream(streamCtx, addresses, StorageLevel.MEMORY_AND_DISK_SER_2, 1000, 1)
    // Utility class for Transforming Log Data
    val transformLog = new ScalaLogAnalyzer()
    // Invoking Flatmap operation to flattening the results and convert them into Key / Value pairs
    val newDstream = flumeStream.flatMap { x =>
      transformLog.transformLogDataIntoSeq(new String(x.event.getBody().array()))
    }
    /** End Common piece of code for all kinds of Output Operations */
    // Define Keyspace
    val keyspaceName = "logdata"
    // Define Table
    val csTableName = "apachelogdata"
    // Invoke saveToCassandra to persist DStream to Cassandra CF
    newDstream.saveToCassandra(keyspaceName, csTableName, SomeColumns("ip", "client", "user", "date", "method", "request", "protocol", "respcode", " size"))
    streamCtx.start()
    streamCtx.awaitTermination()
  }
}
