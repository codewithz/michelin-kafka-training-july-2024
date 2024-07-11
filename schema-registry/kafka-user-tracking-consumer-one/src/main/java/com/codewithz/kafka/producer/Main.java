package com.codewithz.kafka.producer;


import com.codewithz.kafka.model.*;
import com.codewithz.kafka.producer.model.Event;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Properties;

import static java.lang.Thread.sleep;

@Slf4j
public class Main {

    public static void main(String[] args) throws InterruptedException {

        EventGenerator eventGenerator=new EventGenerator();

        String bootstrapServer="127.0.0.1:9093";
        String topic="tkfegbl0.user.tracking";

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "pkc-mvjp7.northeurope.azure.confluent.cloud:9092");
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='V5YLQ3RZZOCZDMX7' password='6o0m73RmDSkGpYhyK3j/lOj0Ofmv+me9hxZBSbC97MYcfckUydDoTA/WlpjBDhnG';");
        properties.put("sasl.mechanism", "PLAIN");
        // Required for correctness in Apache Kafka clients prior to 2.6
        properties.put("client.dns.lookup", "use_all_dns_ips");



        // Create Producer Properties
//        Properties properties=new Properties();
//        properties.
//                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
//                        bootstrapServer);
        properties.
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "io.confluent.kafka.serializers.KafkaAvroSerializer");

        properties.setProperty("schema.registry.url","http://localhost:8081");

        properties.put("schema.registry.url", "https://api.michelin.com/ccloud-schema-registry/dev/");
        properties.put("basic.auth.credentials.source", "USER_INFO");
        properties.put("basic.auth.user.info", "training: training-secret");

        KafkaProducer<User,Product> producer=new KafkaProducer<>(properties);


        for(int index=1;index<=10;index++){
            System.out.println("Event No:"+index);

            Event event=eventGenerator.generateEvent();
            User key=extractKey(event);
            Product value=extractValue(event);

            ProducerRecord<User,Product> record=new ProducerRecord<>(topic, key,value);

            producer.send(record);

            Thread.sleep(1000);

        }
    }

    private static User extractKey(Event event) {
        return User.newBuilder()
                .setUserId(event.getInternalUser().getUserId().toString())
                .setUsername(event.getInternalUser().getUsername())
                .setDateOfBirth((int)event.getInternalUser().getDateOfBirth().toInstant().atZone(ZoneId.systemDefault()).getLong(ChronoField.EPOCH_DAY))
                .build();
    }

    private static Product extractValue(Event event) {
        return Product.newBuilder()
                .setProductType(ProductType.valueOf(event.getInternalProduct().getProductType().name()))
                .setColor(Color.valueOf(event.getInternalProduct().getColor().name()))
                .setDesignType(DesignType.valueOf(event.getInternalProduct().getDesignType().name()))
                .build();
    }

}
