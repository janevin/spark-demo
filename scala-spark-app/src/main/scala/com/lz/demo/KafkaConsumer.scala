package com.lz.demo

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object KafkaConsumer {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint("./checkpoint")

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "192.168.56.3:9092",		// kafka集群地址
            "key.deserializer" -> classOf[StringDeserializer],		// key反序列化规则
            "value.deserializer" -> classOf[StringDeserializer],	// value反序列化规则
            "group.id" -> "1",										// 消费者组
            // earliest：从offset开始消费，没有则从最早消息开始消费
            // latest：从offset开始消费，没有则从最新消息开始消费
            // none：从offset开始消费，没有则报错
            "auto.offset.reset" -> "latest",
            "auto.commit.interval.ms" -> "1000",					// 自动提交offset时间间隔
            "enable.auto.commit" -> (false: java.lang.Boolean)		// 是否自动提交
        )

        val topics = Array("spark-topic")
        val kafkaDs = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,								// 位置策略
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)	// 消费策略
        )

        // 每消费完一小批数据就提交一次offset，在DStream中，一小批的体现是rdd
        kafkaDs.foreachRDD(rdd => {
            if(!rdd.isEmpty()) {
                // 消费
                rdd.foreach(record => {
                    val topic = record.topic()
                    val partition = record.partition()
                    val offset = record.offset()
                    val key = record.key()
                    val value = record.value()
                    val info =
                        s"""
                           |topic: ${topic}, partition: ${partition}, offset: ${offset}, key: ${key}, value: ${value}
                           |""".stripMargin
                    println("消费信息： " + info)
                })
                // 获取rdd中的offset相关的信息：offsetRanges里包含了该批次各个分区的offset信息
                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
                //
                kafkaDs.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
                println("当前批次数据offset已手动提交")
            }
        })

        ssc.start()
        ssc.awaitTermination()
    }
}
