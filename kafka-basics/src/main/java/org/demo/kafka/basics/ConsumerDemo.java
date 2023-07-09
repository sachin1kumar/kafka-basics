package org.demo.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static java.time.Duration.ofMillis;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "kafka application";
        String topic = "demo_java";

        //Create consumer properties.
        Properties properties = new Properties();

        //connect to localhost
        //properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"5RB36gcr1OFKHTkEWByOw5\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI1UkIzNmdjcjFPRktIVGtFV0J5T3c1Iiwib3JnYW5pemF0aW9uSWQiOjc0MzkzLCJ1c2VySWQiOjg2NTQxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiJlMGM2YmNlOC1lMzY3LTRhNmUtOTE0NS0xMjQxZDZkNjRkYjYifX0.ZFOJaaFZcU355eQiPqL1i16fw_1c-GoCl91khj7I8ko\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set consumer properties.
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");


        //create consumer.
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        //subscribe to a topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //poll for data
        while (true) {
            log.info("Polling");
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(ofMillis(1000));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }

    }
}