package com.github.arfinadir.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.Integer;
import java.util.Properties;


public class ProducerDemoWithCallbacks {

    public static void main(String[] args) {
        //create logger
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);

        String bootstrapServer = "127.0.0.1:9092";

        // Create Producer Properties
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);

        // For loop
        for (int i=0;i<4; i++) {
            // Create a Producer record
            String value = "Hello  " + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<String, String>("top_1", value);

            // Send Data asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // record was successfully sent
                        logger.info("Received new metadata. \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Partition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp() + "\n");
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        // End for loop

        // producer.flush();
        producer.close();

    }
}