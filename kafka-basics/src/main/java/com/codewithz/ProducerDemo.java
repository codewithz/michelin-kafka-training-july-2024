package com.codewithz;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
// import org.apache.kafka.common.serialization.Serdes.StringSerde;

public class ProducerDemo {
    public static void main(String[] args) {
        String bootstrapServer="127.0.0.1:9092";
        String topic="michelin_topic";

        // Create Producer Properties
        Properties properties=new Properties();
        properties.
        setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
         bootstrapServer);
        properties.
        setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
         StringSerializer.class.getName());
        properties.
        setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
         StringSerializer.class.getName());

        // Create the Producer 

        KafkaProducer<String,String> producer
        =new KafkaProducer<>(properties);

        // Create a Producer Record 
        ProducerRecord<String,String> record=
        new ProducerRecord<String,String>
        (topic,"Michelin","Message from a Java Producer--1");
        
        // Send the message --> Syncronous Way

        producer.send(record);

        producer.flush();
        }
}
