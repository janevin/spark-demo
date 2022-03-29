package com.lz.demo.dstream.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util

object KafkaProducer {
    def main(args: Array[String]): Unit = {
        val props = new util.HashMap[String, Object]();
        props.put("bootstrap.servers", "192.168.56.3:9092")
        props.put("key.serializer", classOf[StringSerializer])
        props.put("value.serializer", classOf[StringSerializer])

        val producer = new KafkaProducer[String, String](props)
        // 通过producer发送消息
        while (true) {
            val str = (1 to 5).map(_ => scala.util.Random.nextInt(10).toString).mkString("")
            println(str)
            val message = new ProducerRecord[String, String]("spark-topic", "myKey", str)
            producer.send(message)
            Thread.sleep(1000)
        }
    }
}
