package com.codewithz;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
// import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {
        String bootstrapServer="127.0.0.1:9092";
        String topic="michelin_topic_one";

        Logger logger=LoggerFactory
        .getLogger(ProducerDemoWithCallback.class);

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

       for(int key=0;key<=100;key++){
        // Create A Producer Record
        ProducerRecord<String,String> producerRecord=
        new ProducerRecord<String,String>
        (topic, String.valueOf(key),"Michelin-"+key); 

        // Send the data in async way 

        producer.send(producerRecord,new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata,
                                          Exception exception) {
               if(exception==null){
                // REcord is successfully sent
                logger.info("New Meta Data recieved");
                logger.info("Topic:"+recordMetadata.topic());
                logger.info("Partition:"+recordMetadata.partition());
                logger.info("Offset:"+recordMetadata.offset());
                logger.info("Timestamp:"+recordMetadata.timestamp());

               }
               else{
                logger.error("Error while producing the record");
                logger.error(exception.toString());
               }
            }
           
            
        });

        producer.flush();
       }
        }
}
