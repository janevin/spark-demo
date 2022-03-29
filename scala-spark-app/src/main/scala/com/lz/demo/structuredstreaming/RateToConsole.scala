package com.lz.demo.structuredstreaming

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object RateToConsole {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val df: Dataset[Row] = spark.readStream.format("rate")
            .option("rowsPerSecond", "10") 		// 每秒生成数据条数
            .option("rampUpTime", "0s") 		// 每条数据生成间隔时间
            .option("numPartitions", 2) 		// 分区数
            .load()

        df.printSchema()

        df.writeStream
            .format("console")
            .outputMode("append")
            .option("truncate", false)
            .start()
            .awaitTermination()

        spark.stop()
    }
}
