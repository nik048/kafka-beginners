package com.nik.kafkapractice.lesson1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrapServers = "localhost:9092";

        /* producer properties */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /* producer */
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        for(int i=0; i<5; i++){
            String topic = "first_topic";
            String value = "hello world" + i;
            //same key goes to same partition
            //Create multiple partition first
            String key = "id+"+i;
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key, value);

            //wont be very clear as calls are async
            logger.info("key = " + key);

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
                                recordMetadata.topic(),
                                recordMetadata.partition(),
                                recordMetadata.offset(),
                                recordMetadata.timestamp());
                    }
                    else {
                        logger.error("Error producing", e);
                    }
                }
            });
        }


        producer.flush();
        producer.close();
    }
}
