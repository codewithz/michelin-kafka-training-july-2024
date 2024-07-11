package com.codewithz;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo1 {

    public static void main(String[] args) {
        
        Logger logger=LoggerFactory.getLogger(ConsumerDemo.class);
        final String BOOTSTRAP_SERVER="localhost:9092";
        final String TOPIC="michelin_orders";
        final String GROUP_ID="michelin_finance_conusmer";

        // Properties
        Properties properties=new Properties();
        properties.
        setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
        BOOTSTRAP_SERVER);
        properties.
        setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
        StringDeserializer.class.getName());
        properties.
        setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
         DoubleDeserializer.class.getName());
        properties.
        setProperty(ConsumerConfig.GROUP_ID_CONFIG,
         GROUP_ID );
        // properties.
        // setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        // "latest" );
        // earliest/latest/none

        //Create a Consumer
        KafkaConsumer<String,Double> consumer=
        new KafkaConsumer<>(properties);

        // Subscribe to a Topic

        // consumer.subscribe(Collections.singleton(TOPIC));
        consumer.subscribe(Arrays.asList(TOPIC));

        // Poll for New Data 
        while(true){
            ConsumerRecords<String,Double> records=
            consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String,Double> record:records){
                logger.info("---------------------------------");
                logger.info("K:"+record.key() +" || V:"+record.value());
                logger.info("P:"+record.partition() +" || O:"+record.offset());
            }
        }

    
    }
    
}
