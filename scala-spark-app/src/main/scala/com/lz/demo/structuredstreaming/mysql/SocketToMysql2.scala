package com.lz.demo.structuredstreaming.mysql

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object SocketToMysql2 {
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
            .toDF("word", "count")

        val mysqlSink = new MysqlSink("jdbc:mysql://localhost:3306/demo", "root", "123456")

        result.writeStream
            .outputMode("update")
            .foreach(mysqlSink)
            .start()
            .awaitTermination()

        spark.stop()
    }
}
