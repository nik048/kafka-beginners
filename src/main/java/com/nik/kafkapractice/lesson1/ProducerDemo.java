package com.nik.kafkapractice.lesson1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
        String bootstrapServers = "develop50-ucs.cisco.com:9092";

        /* producer properties */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /* producer */
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "hello world");

        /* send */
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e == null){
                    logger.info("received new metadata. \n" +
                            "Topic: {} \n" +
                            "Partition: {} \n" +
                            "Offsets: {}" +
                            "Timestamps: {}",
                            recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                }
                else {
                    logger.error("Error producing", e);
                }
            }
        });

        producer.flush();
        producer.close();
    }
}
