package com.lz.demo.structuredstreaming

import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object FileToConsole {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder().master("local[*]").appName("SparkSQL")
            .config("spark.sql.shuffle.partitions", "2").getOrCreate()

        val schema = new StructType()
            .add("name", StringType, nullable = true)
            .add("age", IntegerType, nullable = true)

        val df: Dataset[Row] = spark.readStream.format("csv")
            .option("sep", ",")
            .option("header", "false")
            .schema(schema)
            .load("data/input/")            // 流式读取当前工程data/input目录下的csv文件

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
