package com.lz.demo.structuredstreaming

import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object SocketToMysql {
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
            .foreachBatch((ds: Dataset[Row], batchId: Long) => {
                ds.show()
                ds.write.mode(SaveMode.Overwrite)
                    .format("jdbc")
                    .option("url", "jdbc:mysql://192.168.56.3:3306/demo?useSSL=false")
                    .option("user", "root")
                    .option("password", "123456")
                    .option("dbtable", "word_count")
                    .save()
            })
            .outputMode("complete")
            .start()
            .awaitTermination()

        spark.stop()
    }
}
