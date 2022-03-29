package com.lz.demo

import org.apache.spark.{SparkConf, SparkContext}

object StreamingSocket {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
        val sc = new SparkContext(conf)

        import org.apache.spark.streaming._
        val ssc = new StreamingContext(sc, Seconds(1))
        val lines = ssc.socketTextStream("192.168.56.3", 9999)
        val words = lines.flatMap(_.split(" "))
        val wordCounts = words.map((_, 1)).reduceByKey(_ + _)
        wordCounts.print()
        ssc.start()
        ssc.awaitTermination()
    }

}
