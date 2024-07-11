package com.codewithz.kafka.producer;

import java.security.cert.LDAPCertStoreParameters;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.codewithz.kafka.model.Color;
import com.codewithz.kafka.model.DesignType;
import com.codewithz.kafka.model.Product;
import com.codewithz.kafka.model.ProductType;
import com.codewithz.kafka.model.User;
import com.codewithz.kafka.producer.model.Event;
import com.codewithz.kafka.producer.model.InternalProduct;
import com.codewithz.kafka.producer.model.InternalUser;

public class MainProducer {
    public static void main(String[] args) throws InterruptedException {
        EventGenerator eventGenerator=new EventGenerator();

         String bootstrapServer="127.0.0.1:9092";
        String topic="user-tracking-one";



        // Create Producer Properties
        Properties properties=new Properties();
        properties.
                setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        bootstrapServer);
        properties.
                setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                        "io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.
                setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                        "io.confluent.kafka.serializers.KafkaAvroSerializer");

        properties.setProperty("schema.registry.url","http://localhost:8081");

        KafkaProducer<User,Product> producer=new KafkaProducer<>(properties);

        for(int eventSeq=1;eventSeq<=20;eventSeq++){
            System.out.println("Event No: "+eventSeq);

            Event event=eventGenerator.generateEvent();
            User key=extractKey(event);
            Product value=extracProduct(event);

            ProducerRecord<User,Product> record=
            new ProducerRecord<User,Product>(topic, key,value);

            producer.send(record);

            Thread.sleep(1000);
        }
    }   

    private static User extractKey(Event event){
        InternalUser internalUser=event.getInternalUser();
        return User.newBuilder()
                .setUserId(internalUser.getUserId().toString())
                .setUsername(internalUser.getUsername())
                .setDateOfBirth((int)internalUser.getDateOfBirth().toInstant().atZone(ZoneId.systemDefault()).getLong(ChronoField.EPOCH_DAY))
                .build();
    }

    private static Product extracProduct(Event event){
        InternalProduct internalProduct=event.getInternalProduct();
        return Product.newBuilder()
                .setProductType(ProductType.valueOf(internalProduct.getProductType().name()))
                .setColor(Color.valueOf(internalProduct.getColor().name()))
                .setDesignType(DesignType.valueOf(internalProduct.getDesignType().name()))
                .build();

    }
}
