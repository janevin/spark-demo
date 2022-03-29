package com.lz.demo.structuredstreaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object SocketToConsole {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val df: Dataset[Row] = spark.readStream.format("socket")
            .option("host", "192.168.56.3")
            .option("port", 9999)
            .load()

        df.printSchema()

        import spark.implicits._

        val ds: Dataset[String] = df.as[String]
        val result: Dataset[Row] = ds.flatMap(_.split(" "))
            .groupBy('value)
            .count()
            .sort($"count".desc)

        result.writeStream
            .format("console")						    // 往控制台写
            .outputMode("complete")				// 每次将所有的数据写出
            .trigger(Trigger.ProcessingTime(0))		// 触发时间间隔，0表示尽可能的快
            .start()
            .awaitTermination()

        spark.stop()
    }
}
