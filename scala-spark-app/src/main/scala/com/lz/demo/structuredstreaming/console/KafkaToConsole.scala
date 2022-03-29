package com.lz.demo.structuredstreaming.console

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object KafkaToConsole {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[2]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val kafkaStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "192.168.56.3:9092")
            .option("subscribe", "topic1")
            .option("startingOffsets", "earliest")
            .load()

        kafkaStream.printSchema()

        import spark.implicits._

        val df: DataFrame = kafkaStream.selectExpr("cast(value as string)").alias("value").where("value is not null")

        val ds: Dataset[String] = df.as[String]
        val result: Dataset[Row] = ds.flatMap(_.split(" "))
            .groupBy('value)
            .count()
            .sort($"count".desc)

        result.writeStream
            .format("console")                      // 往控制台写
            .outputMode("complete")             // 每次将所有的数据写出
            .start()
            .awaitTermination()

        spark.stop()
    }
}
