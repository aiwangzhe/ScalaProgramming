package com.wangzhe.kafka;

import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;

public class MyProducer {

    public static void main(String[] args) {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", "m5.leap.com:6667,m6.leap.com:6667,m7.leap.com:6667");
        properties.put("ack", "all");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer kafkaProducer = new KafkaProducer(properties);

        int i = 0;
        while (i < 100) {
            i++;
            ProducerRecord producerRecord = new ProducerRecord("test1",
                    Integer.toString(i), Integer.toString(i));
            kafkaProducer.send(producerRecord);
        }

        kafkaProducer.close();
    }
}
