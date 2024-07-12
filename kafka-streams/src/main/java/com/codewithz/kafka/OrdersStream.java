package com.codewithz.kafka;

import com.codewithz.kafka.streams.model.Order;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class OrdersStream {

    public static void main(String[] args) {
//         Topics :
//            payments --> validated.payments

//        Message Key --> String -- transactionId
//        Message Value --> Order
//        userId, nbOfItems,totalAmount

        String BOOTSTRAP_SERVERS = "localhost:9092";
        String INPUT_TOPIC = "payments";
        String OUTPUT_TOPIC = "validated.payments";

        Properties properties=new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"orders-stream-app");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        StreamsBuilder builder=new StreamsBuilder();

        KStream<String, Order> stream=builder.stream(INPUT_TOPIC);

        stream.
                peek(OrdersStream::printOnEnter)
                .filter((transactionId,order)->!order.getUserId().toString().equals(""))
                .filter((transactionId,order)->order.getNbOfItems()<1000)
                .filter((transactionId,order)-> order.getTotalAmount()<10000)
                .mapValues(
                        (order)->{
                            order.setUserId(order.getUserId().toString().toUpperCase());
                            return order;
                        }
                )
                .peek(OrdersStream::printOnExit)
                .to(OUTPUT_TOPIC);

        KafkaStreams streams=new KafkaStreams(builder.build(),properties);

        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));



    }

    private static void printOnEnter(String transactionId, Order order) {
        System.out.println("\n*******************************************");
        System.out.println("ENTERING stream transaction with ID < " + transactionId + " >, " +
                "of user < " + order.getUserId() + " >, total amount < " + order.getTotalAmount() +
                " > and nb of items < " + order.getNbOfItems() + " >");
    }

    private static void printOnExit(String transactionId, Order order) {
        System.out.println("EXITING from stream transaction with ID < " + transactionId + " >, " +
                "of user < " + order.getUserId() + " >, total amount < " + order.getTotalAmount() +
                " > and number of items < " + order.getNbOfItems() + " >");
    }
}
