package com.lz.demo.structuredstreaming.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaToKafka {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[2]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()

        val kafkaStream = spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "10.181.41.4:9092")
            .option("subscribe", "topic1")
            .option("startingOffsets", "earliest")
            .load()

        kafkaStream.printSchema()

        val df: DataFrame = kafkaStream.selectExpr("cast(value as string)").alias("value").where("value is not null")

        df.writeStream
            .format("kafka")
            .outputMode("append")
            .option("kafka.bootstrap.servers", "10.181.41.4:9092")
            .option("topic", "topic2")
            .option("checkpointLocation", "checkpoint")
            .start()
            .awaitTermination()

        spark.stop()
    }
}
