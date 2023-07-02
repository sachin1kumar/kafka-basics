package org.demo.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoCallBackWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoCallBackWithKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello world!");

        //Create producer properties.
        Properties properties = new Properties();

        //connect to localhost
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5RB36gcr1OFKHTkEWByOw5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1UkIzNmdjcjFPRktIVGtFV0J5T3c1Iiwib3JnYW5pemF0aW9uSWQiOjc0MzkzLCJ1c2VySWQiOjg2NTQxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJlMGM2YmNlOC1lMzY3LTRhNmUtOTE0NS0xMjQxZDZkNjRkYjYifX0.ZFOJaaFZcU355eQiPqL1i16fw_1c-GoCl91khj7I8ko\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set producer properties.
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer.
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);


        //send data
        for (int i = 0; i < 2; i++) {
            for (int index = 0; index < 10; index++) {

                String key = "key_" + index;
                String value = "hello world_" + index;

                //create a producer record.
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java",
                        key, value);


                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            //the record was successfully sent.
                            log.info("Received new metadata \n" +
                                    "Key: " + key + "\n" +
                                    "Partition: " + metadata.partition() + "\n");
                        } else {
                            log.info("Error while production:", exception);
                        }
                    }
                });

                //tell the producer to send all data and block until done - synchronous.
                kafkaProducer.flush();
            }
        }
        //flush and close the producer.
        kafkaProducer.close();
    }
}